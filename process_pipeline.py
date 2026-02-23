#!/usr/bin/env python3
"""
Comprehensive Email Processing Pipeline

This script processes email data through 4 stages:
1. Filtering - Filters by org_code and removes internal email domains (@qrmhealth.com, @covr.care)
2. Email Verification - Validates remaining emails using local API
3. Enrichment - Adds ClickUp and HubSpot data from BigQuery
4. Login Data - Appends last login and login count data

Usage:
    python process_pipeline.py                    # Run all steps
    python process_pipeline.py --step 1         # Run only step 1 (filtering)
    python process_pipeline.py --step 2         # Run only step 2 (email verification)
    python process_pipeline.py --step 3         # Run only step 3 (enrichment)
    python process_pipeline.py --step 4         # Run only step 4 (login data)
    python process_pipeline.py --skip-api        # Skip API-dependent steps (step 2)
    python process_pipeline.py --skip-bq         # Skip BigQuery-dependent steps (step 3)
"""

import csv
import os
import sys
import argparse
import concurrent.futures
import requests
import json
import re
import logging
from google.cloud import bigquery
import difflib

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

# Input/Output files
INPUT_FILE = 'View Users 251216 - DataIQ Users (2).csv'
LOGIN_DATA_FILE = 'View Product Last Login Data.csv'

# Intermediate files
STEP1_OUTPUT = 'verified_emails_output.csv'
STEP2_OUTPUT = 'cleaned view user list.csv'
STEP3_OUTPUT = 'cleaned view user list_enriched.csv'
FINAL_OUTPUT = 'final_complete_results.csv'

# API Configuration
API_URL = "http://localhost:8080/api/validate"
MAX_WORKERS = 20
BATCH_TIMEOUT = 10

# BigQuery Configuration
BQ_PROJECT_ID = 'gen-lang-client-0844868008'
BQ_LIST_ID = '901302721443'  # Corporations list ID

# Exclusion lists - org codes and corporation names to exclude
# - NEX and CSNHC are already cross-sold
# - DATAIQ entries are demo/test corporations
EXCLUDED_ORG_CODES = {
    "NEX", "CSNHC"
}

EXCLUDED_CORPORATIONS = {
    "DATAIQ (DEMO)", "DATA IQ (ORG)", "DATAIQ (FPA TEST)"
}

# Combined set for easy reference (union of both)
ALL_EXCLUDED = EXCLUDED_ORG_CODES | EXCLUDED_CORPORATIONS
ORG_CODE_ALIASES = {
    'PHGUS': 'CCS',
    'PHCLA': 'PMHCC',
    'ALVCC': 'PCHM'
}
CORP_NAME_ALIASES = {
    'Nava Healthcare / Arabella': 'Nava Healthcare',
    'PacifiCare Health Management (ALVCC)': 'PacifiCare Health Management',
    'Vivage': 'Vivage Management',
    'Judson Village': 'Hillstone'
}


def get_org_codes_from_clickup():
    """
    Query ClickUp to dynamically determine org codes and campaign categories:
    
    1. VIEW_CLINICAL_ORG_CODES (View Clinical - Labor + Flow expansion):
       - Customer Type = "View"
       - Services NOT equal to "Labor" AND NOT equal to "QRM"
       - Status = "active"
    
    2. QRM_CADENCE_ORG_CODES (Corporate Cadence for QRM customers):
       - Customer Type = "View"
       - Services = "QRM" (and NOT "Labor")
       - Status = "active"
    
    3. OTHER_ACTIVE_ORG_CODES (All other active corporations):
       - Customer Type = "View"
       - Status = "active" OR "implementation"
    
    4. LOSING_ACCESS_QRM_ORG_CODES (Losing access to QRM reports):
       - Sales Outreach Campaign = "Pitch SNF Metrics (QRM Downsell)"
       - Customer Type = "View"
       - Status = "active"
    
    5. CORPORATE_CADENCE_LABOR_ORG_CODES (Corporate Cadence for customers who pay for labor reports):
       - Customer Type = "View"
       - Services includes "Labor"
       - Status = "active"
    
    Returns:
        - view_clinical_orgs: set of org codes for View Clinical
        - qrm_cadence_orgs: set of org codes for QRM Cadence
        - other_active_orgs: set of org codes for Other Active
        - losing_access_qrm_orgs: set of org codes for Losing access to QRM reports
        - corporate_cadence_labor_orgs: set of org codes for Corporate Cadence (Labor)
    """
    client = bigquery.Client(project=BQ_PROJECT_ID)
    
    query = f"""
        SELECT id, name, status, custom_fields
        FROM `gen-lang-client-0844868008.ClickUp_AirbyteCustom.task`
        WHERE JSON_VALUE(list, '$.id') = '{BQ_LIST_ID}'
    """
    
    logger.info("Querying ClickUp for org code categorization...")
    query_job = client.query(query)
    results = query_job.result()
    
    view_clinical_orgs = set()   # View + NOT Labor + NOT QRM + active
    qrm_cadence_orgs = set()    # View + QRM + NOT Labor + active
    other_active_orgs = set()    # View + active
    in_implementation_orgs = set()  # View + implementation
    losing_access_qrm_orgs = set()  # Losing access to QRM reports
    corporate_cadence_labor_orgs = set()  # Corporate Cadence for Labor
    
    for row in results:
        task_id = row.id
        task_name = row.name
        status_json = row.status
        
        # Parse status
        status_val = 'unknown'
        if isinstance(status_json, str):
            try:
                s_dict = json.loads(status_json)
                status_val = s_dict.get('status', '').lower()
            except:
                pass
        elif isinstance(status_json, dict):
            status_val = status_json.get('status', '').lower()
        
        # Parse custom fields
        cfields_raw = row.custom_fields
        cfields = []
        if isinstance(cfields_raw, str):
            try:
                cfields = json.loads(cfields_raw)
            except:
                continue
        elif isinstance(cfields_raw, list):
            cfields = cfields_raw
        
        # Extract Customer Type
        # The value can be an integer (orderindex) or a string ID
        customer_type = ''
        customer_type_field = next((f for f in cfields if f.get('name') == 'Customer Type'), None)
        if customer_type_field:
            value = customer_type_field.get('value', None)
            options = customer_type_field.get('type_config', {}).get('options', [])
            
            if value is not None:
                # Check if value is an orderindex (integer) - look up by orderindex
                if isinstance(value, int):
                    for opt in options:
                        if opt.get('orderindex') == value:
                            customer_type = opt.get('name', '').lower()
                            break
                else:
                    # Value is a string ID - look up by id
                    for opt in options:
                        if str(opt.get('id')) == str(value):
                            customer_type = opt.get('name', '').lower()
                            break
        
        # Check for "View + Flow" (or "Flow + View") - these are cross-sold
        is_view_plus_flow = 'view' in customer_type and 'flow' in customer_type
        
        # Only process exact "View" customer types
        if customer_type != 'view':
            # Skip "View + Flow" (cross-sold), "Flow", "Sync", etc.
            # Exception: SLP is a special case we want to include
            continue
        
        # Extract Services - value is an array of service IDs
        services = set()
        services_field = next((f for f in cfields if f.get('name') == 'Services'), None)
        if services_field:
            service_value = services_field.get('value', [])
            type_config = services_field.get('type_config', {})
            
            if isinstance(service_value, list):
                # Build a lookup from ID to label
                id_to_label = {}
                for opt in type_config.get('options', []):
                    id_to_label[opt.get('id', '')] = opt.get('label', '').lower()
                
                for service_id in service_value:
                    label = id_to_label.get(service_id, '')
                    if label:
                        services.add(label)
            elif isinstance(service_value, str) and service_value:
                # Single value
                id_to_label = {}
                for opt in type_config.get('options', []):
                    id_to_label[opt.get('id', '')] = opt.get('label', '').lower()
                label = id_to_label.get(service_value, '')
                if label:
                    services.add(label)
        
        # Extract Sales Outreach Campaign (type is "labels" - value is an array of label IDs)
        sales_outreach_campaigns = []
        sales_outreach_field = next((f for f in cfields if f.get('name') == 'Sales Outreach Campaign'), None)
        if sales_outreach_field:
            # For labels type, value is an array of label IDs
            value = sales_outreach_field.get('value', [])
            if isinstance(value, str):
                value = [value]
            elif not isinstance(value, list):
                value = []
            
            # Build lookup from ID to label name
            id_to_label = {}
            for opt in sales_outreach_field.get('type_config', {}).get('options', []):
                id_to_label[opt.get('id', '')] = opt.get('label', '').lower()
            
            # Get all selected label names
            for label_id in value:
                label = id_to_label.get(label_id, '')
                if label:
                    sales_outreach_campaigns.append(label)
        
        # Check for "Pitch SNF Metrics (QRM Downsell)" - case insensitive partial match
        is_qrm_downsell = any('pitch snf metrics' in camp or 'qrm downsell' in camp for camp in sales_outreach_campaigns)
        
        # Extract Org Codes - value is an array of org code IDs
        org_codes = set()
        field = next((f for f in cfields if f.get('name') == 'Org Code'), None)
        if not field:
            continue
        
        value_ids = field.get('value', [])
        if not value_ids:
            continue
        
        if isinstance(value_ids, str):
            value_ids = [value_ids]
        elif not isinstance(value_ids, list):
            value_ids = [value_ids]
        
        # Build lookup from ID to label
        options = field.get('type_config', {}).get('options', [])
        id_to_label = {}
        for opt in options:
            id_to_label[opt.get('id', '')] = opt.get('label', '')
        
        for val in value_ids:
            label = id_to_label.get(val, '')
            if label:
                # Normalize and extract individual words as org codes
                normalized = label.replace(',', ' ').replace('-', ' ').replace('/', ' ')
                words = normalized.split()
                for word in words:
                    clean_code = word.strip().upper()
                    if clean_code and not clean_code.startswith('<') and len(clean_code) > 1:
                        org_codes.add(clean_code)
        
        # Determine category based on criteria
        has_labor = 'labor' in services
        has_qrm = 'qrm' in services
        has_mds = 'mds' in services
        is_active = status_val == 'active'
        is_implementation = status_val == 'implementation'
        
        for org_code in org_codes:
            # Losing access to QRM reports: Sales Outreach Campaign = "Pitch SNF Metrics (QRM Downsell)" + active
            if is_qrm_downsell and is_active:
                losing_access_qrm_orgs.add(org_code)
            # Corporate Cadence for Labor: View + Labor + active
            elif has_labor and is_active:
                corporate_cadence_labor_orgs.add(org_code)
            # Corporate Cadence for QRM MDS customers: View + ONLY QRM + MDS (no other services) + active
            elif has_qrm and has_mds and len(services) == 2 and is_active:
                qrm_cadence_orgs.add(org_code)
            # View Clinical: View + active + NOT in any other campaign (QRM MDS or Losing Access QRM)
            elif is_active:
                # Only add to View Clinical if not already in another campaign
                if org_code not in losing_access_qrm_orgs and org_code not in corporate_cadence_labor_orgs and org_code not in qrm_cadence_orgs:
                    view_clinical_orgs.add(org_code)
            # In Implementation: View + implementation status
            elif is_implementation:
                in_implementation_orgs.add(org_code)
            # Other Active: View + active (fallback)
            elif is_active:
                other_active_orgs.add(org_code)
    
    logger.info(f"Found {len(view_clinical_orgs)} View Clinical org codes: {list(view_clinical_orgs)[:10]}...")
    logger.info(f"Found {len(qrm_cadence_orgs)} QRM Cadence org codes: {list(qrm_cadence_orgs)[:10]}...")
    logger.info(f"Found {len(other_active_orgs)} Other Active org codes: {list(other_active_orgs)[:10]}...")
    logger.info(f"Found {len(in_implementation_orgs)} In Implementation org codes: {list(in_implementation_orgs)[:10]}...")
    logger.info(f"Found {len(losing_access_qrm_orgs)} Losing access to QRM reports org codes: {list(losing_access_qrm_orgs)[:10]}...")
    logger.info(f"Found {len(corporate_cadence_labor_orgs)} Corporate Cadence Labor org codes: {list(corporate_cadence_labor_orgs)[:10]}...")
    
    return view_clinical_orgs, qrm_cadence_orgs, other_active_orgs, in_implementation_orgs, losing_access_qrm_orgs, corporate_cadence_labor_orgs

# =============================================================================
# STEP 1: FILTERING
# =============================================================================

# Internal email domains to remove (these are company emails that don't need validation)
INTERNAL_EMAIL_DOMAINS = {'qrmhealth.com', 'covr.care', 'dataiqbi.com', 'softserveinc.com'}

def step1_filter_rows(use_dynamic_org_codes=True):
    """Step 1: Filter by org_code and remove internal email domains.
    
    This step:
    - Removes rows with excluded org codes
    - Removes rows with internal email domains (@qrmhealth.com, @covr.care)
    - Classifies View User types and Campaign
    
    Args:
        use_dynamic_org_codes: If True, query ClickUp to dynamically determine
            org code categories. If False, use hardcoded sets.
    """
    logger.info("=" * 50)
    logger.info("STEP 1: Filtering (Org Codes + Internal Emails)")
    logger.info("=" * 50)
    
    if not os.path.exists(INPUT_FILE):
        logger.error(f"Input file {INPUT_FILE} not found.")
        return False
    
    # Get org codes (either dynamic from ClickUp or hardcoded)
    if use_dynamic_org_codes:
        logger.info("Fetching org codes dynamically from ClickUp...")
        view_clinical_codes, qrm_cadence_codes, other_active_codes, in_implementation_codes, losing_access_qrm_codes, corporate_cadence_labor_codes = get_org_codes_from_clickup()
    else:
        logger.info("Using hardcoded org code sets...")
        view_clinical_codes = VIEW_LABOR_ORG_CODES
        qrm_cadence_codes = QRM_LABOR_ORG_CODES
        other_active_codes = CHURNED_VIEW_CORP_ORG_CODES
        in_implementation_codes = set()  # No hardcoded set for this
        losing_access_qrm_codes = set()  # No hardcoded set for this
        corporate_cadence_labor_codes = set()  # No hardcoded set for this
    
    # Combine all active org codes (those to KEEP)
    active_org_codes = view_clinical_codes | qrm_cadence_codes | other_active_codes | in_implementation_codes | losing_access_qrm_codes | corporate_cadence_labor_codes
    
    try:
        with open(INPUT_FILE, mode='r', newline='', encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)
            
            if 'email' not in reader.fieldnames:
                logger.error(f"'email' column not found in {INPUT_FILE}")
                return False
            
            # Add View User type and Campaign columns
            fieldnames = list(reader.fieldnames) + ['View User type', 'campaign']
            
            with open(STEP1_OUTPUT, mode='w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()
                
                count_kept = 0
                count_removed_org = 0
                count_removed_internal = 0
                count_removed_total = 0
                
                for row in reader:
                    org_code = row.get('org_code', '').strip().upper()
                    email = row.get('email', '').strip().lower()
                    
                    # Extract domain from email
                    email_domain = ''
                    if '@' in email:
                        email_domain = email.split('@')[1]
                    
                    # Check if internal email domain
                    is_internal = email_domain in INTERNAL_EMAIL_DOMAINS
                    
                    # Check if org code is in active list (or if org_code is empty)
                    # Keep rows where org_code is either empty or in active_org_codes
                    org_in_active = (org_code == '') or (org_code in active_org_codes)
                    
                    # Determine campaign category
                    # Priority order: Losing Access QRM > Customers who pay for labor reports > QRM MDS Cadence > View Clinical > In Implementation > Other Active
                    campaign = ''
                    if org_code in losing_access_qrm_codes:
                        campaign = "Losing access to QRM reports"
                    elif org_code in corporate_cadence_labor_codes:
                        campaign = "customers who pay for labor reports (Selling Flow)"
                    elif org_code in view_clinical_codes:
                        campaign = "View Clinical customers NOT using Labor (Labor + Flow expansion)"
                    elif org_code in qrm_cadence_codes:
                        campaign = "Corporate Cadence for QRM MDS customers (Selling View labor expansion + Flow)"
                    elif org_code in in_implementation_codes:
                        campaign = "In Implementation"
                    elif org_code in other_active_codes:
                        campaign = "Other Active"
                    
                    # Determine View User type (facility vs corp)
                    if campaign:
                        facilities = row.get('facilities', '').strip()
                        if not facilities or ',' in facilities:
                            view_user_type = f"{campaign} - Corp"
                        else:
                            view_user_type = f"{campaign} - facility"
                    else:
                        view_user_type = "View - No Labor"
                    
                    # Apply filters: Skip if internal email OR org not in active list
                    if is_internal:
                        count_removed_internal += 1
                        continue
                    
                    if not org_in_active:
                        count_removed_org += 1
                        continue
                    
                    # Row passes filters - write it
                    row['View User type'] = view_user_type
                    row['campaign'] = campaign
                    writer.writerow(row)
                    count_kept += 1
        
        count_removed_total = count_removed_org + count_removed_internal
        logger.info(f"Step 1 complete.")
        logger.info(f"Rows kept: {count_kept}")
        logger.info(f"Rows removed (internal email): {count_removed_internal}")
        logger.info(f"Rows removed (excluded org): {count_removed_org}")
        logger.info(f"Total rows removed: {count_removed_total}")
        logger.info(f"Output: {STEP1_OUTPUT}")
        return True
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return False

# =============================================================================
# STEP 2: EMAIL VERIFICATION
# =============================================================================

def verify_email(email):
    """Verifies a single email against the local API."""
    try:
        response = requests.get(API_URL, params={'email': email}, timeout=BATCH_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        validations = data.get('validations', {})
        return {
            'validation_status': data.get('status', 'UNKNOWN'),
            'validation_score': data.get('score', 0),
            'syntax': validations.get('syntax'),
            'domain_exists': validations.get('domain_exists'),
            'mx_records': validations.get('mx_records'),
            'is_disposable': validations.get('is_disposable'),
            'is_role_based': validations.get('is_role_based'),
            'alias_of': data.get('aliasOf', ''),
            'typo_suggestion': data.get('typoSuggestion', ''),
            'validation_error': ''
        }
    except requests.exceptions.ConnectionError:
        # API server not running - return unknown status
        return {
            'validation_status': 'UNKNOWN',
            'validation_score': 0,
            'syntax': None,
            'domain_exists': None,
            'mx_records': None,
            'is_disposable': None,
            'is_role_based': None,
            'alias_of': '',
            'typo_suggestion': '',
            'validation_error': 'API server not available'
        }
    except requests.exceptions.Timeout:
        # API timeout
        return {
            'validation_status': 'TIMEOUT',
            'validation_score': 0,
            'syntax': None,
            'domain_exists': None,
            'mx_records': None,
            'is_disposable': None,
            'is_role_based': None,
            'alias_of': '',
            'typo_suggestion': '',
            'validation_error': 'API request timeout'
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"Error verifying {email}: {e}")
        return {
            'validation_status': 'ERROR',
            'validation_score': 0,
            'syntax': False,
            'domain_exists': False,
            'mx_records': False,
            'is_disposable': False,
            'is_role_based': False,
            'alias_of': '',
            'typo_suggestion': '',
            'validation_error': str(e)
        }

def step2_verify_emails():
    """Step 2: Verify emails using local API.
    
    This step only runs on the filtered rows from Step 1,
    which excludes internal email domains (@qrmhealth.com, @covr.care).
    """
    logger.info("=" * 50)
    logger.info("STEP 2: Email Verification")
    logger.info("=" * 50)
    
    if not os.path.exists(STEP1_OUTPUT):
        logger.error(f"Input file {STEP1_OUTPUT} not found. Run step 1 first.")
        return False
    
    # Read all rows
    rows = []
    email_col = None
    
    try:
        with open(STEP1_OUTPUT, mode='r', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            for field in reader.fieldnames:
                if field.lower() == 'email':
                    email_col = field
                    break
            
            if not email_col:
                logger.error(f"Column 'email' not found in CSV.")
                return False
            
            for row in reader:
                rows.append(row)
    except Exception as e:
        logger.error(f"Failed to read CSV: {e}")
        return False
    
    logger.info(f"Loaded {len(rows)} rows to process.")
    
    validation_fieldnames = [
        'validation_status', 'validation_score', 'syntax', 'domain_exists', 
        'mx_records', 'is_disposable', 'is_role_based', 
        'alias_of', 'typo_suggestion', 'validation_error'
    ]
    
    output_fieldnames = list(rows[0].keys()) if rows else []
    output_fieldnames += [f for f in validation_fieldnames if f not in output_fieldnames]
    
    processed_count = 0
    
    with open(STEP2_OUTPUT, mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=output_fieldnames)
        writer.writeheader()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_row = {}
            for row in rows:
                email = row.get(email_col)
                if email:
                    future = executor.submit(verify_email, email)
                    future_to_row[future] = row
                else:
                    writer.writerow(row)
                    processed_count += 1
            
            for future in concurrent.futures.as_completed(future_to_row):
                row = future_to_row[future]
                try:
                    validation_result = future.result()
                    row.update(validation_result)
                    writer.writerow(row)
                    outfile.flush()
                    processed_count += 1
                    if processed_count % 100 == 0:
                        logger.info(f"Processed {processed_count}/{len(rows)}")
                except Exception as exc:
                    logger.error(f"Row generated an exception: {exc}")
                    row['validation_error'] = str(exc)
                    writer.writerow(row)
    
    logger.info(f"Step 2 complete. Output: {STEP2_OUTPUT}")
    return True

# =============================================================================
# STEP 3: ENRICHMENT (BIGQUERY)
# =============================================================================

def get_clickup_maps(hubspot_companies=None):
    """Fetch ClickUp tasks and build lookup maps.
    
    Args:
        hubspot_companies: Optional pre-fetched HubSpot companies dict to avoid duplicate queries.
                          If not provided, will fetch internally (for backward compatibility).
    """
    client = bigquery.Client(project=BQ_PROJECT_ID)
    
    # Only fetch if not provided
    if hubspot_companies is None:
        hubspot_companies = get_hubspot_companies()
    
    query = f"""
        SELECT id, name, status, custom_fields
        FROM `gen-lang-client-0844868008.ClickUp_AirbyteCustom.task`
        WHERE JSON_VALUE(list, '$.id') = '{BQ_LIST_ID}'
    """
    
    logger.info("Fetching ClickUp tasks from BigQuery...")
    query_job = client.query(query)
    results = query_job.result()
    
    org_map = {}
    name_list = []
    task_map = {}
    
    for row in results:
        task_id = row.id
        task_name = row.name
        status_json = row.status
        
        status_val = 'unknown'
        if isinstance(status_json, str):
            try:
                s_dict = json.loads(status_json)
                status_val = s_dict.get('status')
            except:
                pass
        elif isinstance(status_json, dict):
            status_val = status_json.get('status')
        
        cfields_raw = row.custom_fields
        cfields = []
        if isinstance(cfields_raw, str):
            try:
                cfields = json.loads(cfields_raw)
            except:
                continue
        elif isinstance(cfields_raw, list):
            cfields = cfields_raw
        
        customer_type = ''
        customer_type_field = next((f for f in cfields if f.get('name') == 'Customer Type'), None)
        if customer_type_field:
            value_ids = customer_type_field.get('value', [])
            if isinstance(value_ids, (str, int)):
                value_ids = [value_ids]
            elif not isinstance(value_ids, list):
                value_ids = [value_ids]
            if value_ids:
                options = customer_type_field.get('type_config', {}).get('options', [])
                for opt in options:
                    if str(opt.get('id')) in [str(v) for v in value_ids]:
                        customer_type = opt.get('label', '')
                        break
        
        # Extract Services field (e.g., "MDS, QRM")
        services = ''
        services_field = next((f for f in cfields if f.get('name') == 'Services'), None)
        if services_field:
            service_value = services_field.get('value', '')
            type_config = services_field.get('type_config', {})
            if isinstance(service_value, list) and type_config.get('options'):
                service_names = []
                for service_id in service_value:
                    for option in type_config.get('options', []):
                        if option.get('id') == service_id:
                            service_names.append(option.get('label', service_id))
                            break
                services = ', '.join(service_names) if service_names else str(service_value)
            elif service_value:
                services = str(service_value)
        
        hubspot_url = ''
        hubspot_field = next((f for f in cfields if f.get('name') == 'Hubspot URL'), None)
        if hubspot_field:
            hubspot_url = hubspot_field.get('value', '')
            if isinstance(hubspot_url, list) and hubspot_url:
                hubspot_url = hubspot_url[0]
        
        hubspot_record_id = ''
        hubspot_company = ''
        if hubspot_url:
            url_parts = hubspot_url.rstrip('/').split('/')
            if url_parts:
                hubspot_record_id = url_parts[-1]
                hubspot_company = hubspot_companies.get(hubspot_record_id, '')
        
        # Include services in task_info
        task_info = (task_id, status_val, customer_type, hubspot_url, hubspot_record_id, hubspot_company, services)
        task_map[task_id] = (customer_type, hubspot_url, hubspot_record_id, hubspot_company, services)
        
        if task_name:
            norm_name = task_name.strip().upper()
            original_name = task_name.strip()
            name_list.append((original_name, norm_name, task_id, status_val, customer_type, hubspot_url, hubspot_record_id, hubspot_company, services))
        
        field = next((f for f in cfields if f.get('name') == 'Org Code'), None)
        if not field:
            continue
        
        value_ids = field.get('value', [])
        if isinstance(value_ids, (str, int)):
            value_ids = [value_ids]
        elif not isinstance(value_ids, list):
            value_ids = [value_ids]
        if not value_ids:
            continue
        
        options = field.get('type_config', {}).get('options', [])
        
        for opt in options:
            if str(opt.get('id')) in [str(v) for v in value_ids]:
                label = opt.get('label', '')
                normalized = label.replace(',', ' ').replace('-', ' ').replace('/', ' ')
                words = normalized.split()
                
                for word in words:
                    clean_code = word.strip().upper()
                    if clean_code:
                        org_map[clean_code] = task_info
    
    return org_map, name_list, task_map

def get_hubspot_companies():
    """Fetch HubSpot companies from BigQuery."""
    client = bigquery.Client(project=BQ_PROJECT_ID)
    
    query = """
        SELECT id, properties_name
        FROM `gen-lang-client-0844868008.HubSpot_Airbyte.companies`
        WHERE properties_name IS NOT NULL
    """
    
    logger.info("Fetching HubSpot companies from BigQuery...")
    query_job = client.query(query)
    results = query_job.result()
    
    hubspot_map = {}
    for row in results:
        hubspot_map[str(row.id)] = row.properties_name.strip() if row.properties_name else ''
    
    logger.info(f"Fetched {len(hubspot_map)} HubSpot companies.")
    return hubspot_map


def get_hubspot_contacts_batch_param(batch):
    """Fetch a batch of HubSpot contacts using parameterized query with arrays.
    
    This is much more efficient than string concatenation:
    - Uses IN UNNEST(@emails) instead of building OR conditions
    - Supports larger batch sizes (1000+ emails)
    - Prevents SQL injection
    """
    client = bigquery.Client(project=BQ_PROJECT_ID)
    
    # Use parameterized query with array - much more efficient!
    query = """
        SELECT
            properties_email as email,
            id,
            properties_firstname as first_name,
            properties_lastname as last_name
        FROM
            `gen-lang-client-0844868008.HubSpot_Airbyte.contacts`
        WHERE
            properties_email IN UNNEST(@emails)
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("emails", "STRING", list(batch))
        ]
    )
    
    batch_results = {}
    try:
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()
        
        for row in results:
            email = row.email if hasattr(row, 'email') else ''
            if email:
                batch_results[email.lower()] = {
                    'contact_id': row.id if hasattr(row, 'id') else '',
                    'first_name': row.first_name if hasattr(row, 'first_name') else '',
                    'last_name': row.last_name if hasattr(row, 'last_name') else ''
                }
    except Exception as e:
        logger.warning(f"Error querying HubSpot contacts batch: {e}")
    
    return batch_results


def get_hubspot_contacts(emails, max_workers=10):
    """
    Fetch HubSpot contacts from BigQuery by email addresses.
    Returns contact information for individual people.
    
    Uses parameterized queries with arrays for better performance:
    - IN UNNEST(@emails) instead of OR conditions
    - Larger batch sizes (1000 emails per batch)
    - Parallel execution with ThreadPoolExecutor
    
    Args:
        emails: Set or list of email addresses to lookup
        max_workers: Number of parallel queries (default 10)
    """
    if not emails:
        return {}
    
    email_list = list(emails)
    hubspot_contacts = {}
    
    # Use much larger batches with parameterized queries
    # Previous: 100 emails per batch (77 queries for 7675 emails)
    # Now: 1000 emails per batch (8 queries for 7675 emails)
    batch_size = 1000
    batches = [email_list[i:i+batch_size] for i in range(0, len(email_list), batch_size)]
    
    logger.info(f"Fetching HubSpot contacts in {len(batches)} parallel batches ({batch_size} emails each)...")
    
    # Execute batches in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(get_hubspot_contacts_batch_param, batch) for batch in batches]
        
        for future in concurrent.futures.as_completed(futures):
            try:
                batch_results = future.result()
                hubspot_contacts.update(batch_results)
            except Exception as e:
                logger.warning(f"Error in batch: {e}")
    
    logger.info(f"Fetched {len(hubspot_contacts)} HubSpot contacts.")
    return hubspot_contacts

def step3_enrich_csv():
    """Step 3: Enrich with ClickUp and HubSpot data from BigQuery."""
    logger.info("=" * 50)
    logger.info("STEP 3: Enrichment")
    logger.info("=" * 50)
    
    if not os.path.exists(STEP2_OUTPUT):
        logger.error(f"Input file {STEP2_OUTPUT} not found. Run step 2 first.")
        return False
    
    try:
        # First, collect all emails from the CSV for HubSpot contacts lookup
        all_emails = set()
        with open(STEP2_OUTPUT, mode='r', newline='', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                email = row.get('email', '').strip()
                if email:
                    all_emails.add(email.lower())
        
        logger.info(f"Collected {len(all_emails)} unique emails for HubSpot contact lookup")
        
        # OPTIMIZATION: Fetch HubSpot companies ONCE and reuse
        # Previously this was fetched twice - once in get_clickup_maps() and once here
        logger.info("Fetching HubSpot companies (single query)...")
        hubspot_companies = get_hubspot_companies()
        
        # Fetch HubSpot contacts by email (parallelized)
        hubspot_contacts = get_hubspot_contacts(all_emails)
        
        # Get ClickUp data - pass hubspot_companies to avoid duplicate query
        org_map, name_list, task_map = get_clickup_maps(hubspot_companies)
        logger.info(f"Built lookup data: {len(org_map)} org codes, {len(name_list)} task names")
        
        # Process the CSV
        with open(STEP2_OUTPUT, mode='r', newline='', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            fieldnames = list(reader.fieldnames)
            
            # Add new columns (including services and hubspot contact info)
            new_columns = [
                'task_id', 'task_status', 'customer_type', 'services', 'hubspot_url',
                'hubspot corporation record id', 'hubspot corporation name',
                'facility_task_id', 'facility_task_name', 'facility_corporation_task',
                'facility_corporation_name', 'facility_hubspot_url',
                'facility_hubspot_record_id', 'facility_hubspot_company',
                'hubspot_contact_id', 'hubspot_contact_first_name', 'hubspot_contact_last_name',
                'match_method', 'job title'
            ]
            
            for col in new_columns:
                if col not in fieldnames:
                    fieldnames.append(col)
            
            with open(STEP3_OUTPUT, mode='w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()
                
                matched_org = 0
                matched_name_fuzzy = 0
                matched_alias = 0
                total_count = 0
                excluded_count = 0
                
                # Count total rows first for progress reporting
                total_rows = sum(1 for _ in reader)
                infile.seek(0)
                reader = csv.DictReader(infile)
                
                for idx, row in enumerate(reader):
                    # Progress logging every 500 rows
                    if idx > 0 and idx % 500 == 0:
                        logger.info(f"Step 3 progress: {idx}/{total_rows} rows processed")
                        outfile.flush()  # Ensure progress is written to disk
                    
                    # Get email for HubSpot contact lookup
                    email = row.get('email', '').strip().lower()
                    
                    # Extract job title from last_name
                    last_name = row.get('last_name', '')
                    job_match = re.search(r'\((.*?)\)', last_name)
                    if job_match:
                        row['job title'] = job_match.group(1)
                        row['last_name'] = re.sub(r'\(.*?\)', '', last_name).strip()
                    else:
                        row['job title'] = ''
                    
                    covr_corp = row.get('covr_corporation', '').strip()
                    
                    # Filter excluded corporations
                    if covr_corp.upper() in [ex.upper() for ex in EXCLUDED_CORPORATIONS]:
                        excluded_count += 1
                        continue
                    
                    total_count += 1
                    user_type = row.get('View User type', '').strip()
                    campaign = row.get('campaign', '').strip()
                    
                    # Get campaign for enrichment - include users with any campaign value
                    has_campaign = bool(campaign)
                    
                    # Skip matching only for users without a campaign
                    if not has_campaign:
                        for col in new_columns:
                            if col != 'job title':
                                row[col] = ''
                        writer.writerow(row)
                        continue
                    
                    org_code = row.get('org_code', '').strip().upper()
                    norm_covr_corp = covr_corp.upper()
                    
                    task_id = ''
                    task_status = ''
                    customer_type = ''
                    services = ''
                    hubspot_url = ''
                    hubspot_corp_record_id = ''
                    hubspot_corp_name = ''
                    method = ''
                    
                    # 1. Try Manual Alias
                    if not task_id and org_code in ORG_CODE_ALIASES:
                        alias_target = ORG_CODE_ALIASES[org_code]
                        task_info = org_map.get(alias_target)
                        if task_info:
                            task_id, task_status, customer_type, hubspot_url, hubspot_corp_record_id, hubspot_corp_name, services = task_info
                            method = f'alias({alias_target})'
                            matched_alias += 1
                    
                    # 2. Try Org Code
                    if not task_id and org_code:
                        task_info = org_map.get(org_code)
                        if task_info:
                            task_id, task_status, customer_type, hubspot_url, hubspot_corp_record_id, hubspot_corp_name, services = task_info
                            method = 'org_code'
                            matched_org += 1
                    
                    # 3. Try Corporation Name Alias
                    if not task_id and norm_covr_corp in CORP_NAME_ALIASES:
                        alias_name = CORP_NAME_ALIASES[norm_covr_corp].upper()
                        for item in name_list:
                            cu_orig_name, cu_norm_name, cu_id, cu_status, cu_customer_type, cu_hubspot_url, cu_hubspot_corp_record_id, cu_hubspot_corp_name, cu_services = item
                            if cu_norm_name == alias_name:
                                task_id = cu_id
                                task_status = cu_status
                                customer_type = cu_customer_type
                                hubspot_url = cu_hubspot_url
                                hubspot_corp_record_id = cu_hubspot_corp_record_id
                                hubspot_corp_name = cu_hubspot_corp_name
                                services = cu_services
                                method = f'name_alias({alias_name})'
                                matched_name_fuzzy += 1
                                break
                    
                    # 4. Fuzzy Name Match
                    if not task_id and norm_covr_corp:
                        for item in name_list:
                            cu_orig_name, cu_norm_name, cu_id, cu_status, cu_customer_type, cu_hubspot_url, cu_hubspot_corp_record_id, cu_hubspot_corp_name, cu_services = item
                            if (norm_covr_corp in cu_norm_name) or (cu_norm_name in norm_covr_corp):
                                task_id = cu_id
                                task_status = cu_status
                                customer_type = cu_customer_type
                                hubspot_url = cu_hubspot_url
                                hubspot_corp_record_id = cu_hubspot_corp_record_id
                                hubspot_corp_name = cu_hubspot_corp_name
                                services = cu_services
                                method = 'name_fuzzy_match'
                                matched_name_fuzzy += 1
                                break
                    
                    # Facility matching
                    facility_task_id = ''
                    facility_task_name = ''
                    facility_corporation_task = ''
                    facility_corporation_name = ''
                    facility_hubspot_url = ''
                    facility_hubspot_record_id = ''
                    facility_hubspot_company = ''
                    
                    facilities = row.get('facilities', '').strip()
                    # Check if this is a facility-type user (has campaign and single facility)
                    is_facility_type = campaign and 'facility' in user_type.lower() and facilities and ',' not in facilities
                    
                    if is_facility_type:
                        norm_facility = facilities.upper()
                        for item in name_list:
                            cu_orig_name, cu_norm_name, cu_id, cu_status, cu_customer_type, cu_hubspot_url, cu_hubspot_record_id, cu_hubspot_company, cu_services = item
                            if (norm_facility in cu_norm_name) or (cu_norm_name in norm_facility):
                                facility_task_id = cu_id
                                facility_task_name = cu_orig_name
                                facility_hubspot_url = cu_hubspot_url
                                facility_hubspot_record_id = cu_hubspot_record_id
                                facility_hubspot_company = cu_hubspot_company
                                facility_corporation_task = task_id if task_id else ''
                                break
                        
                        if facility_corporation_task:
                            for item in name_list:
                                cu_orig_name, cu_norm_name, cu_id, cu_status, cu_customer_type, cu_hubspot_url, cu_hubspot_record_id, cu_hubspot_company, cu_services = item
                                if cu_id == facility_corporation_task:
                                    facility_corporation_name = cu_orig_name
                                    break
                    
                    # HubSpot Company Lookup for Facility Names (for all facility types)
                    if is_facility_type:
                        facility_name = row.get('facilities', '').strip()
                        if facility_name:
                            hubspot_names = list(hubspot_companies.values())
                            matches = difflib.get_close_matches(facility_name, hubspot_names, n=1, cutoff=0.6)
                            if matches:
                                matched_name = matches[0]
                                for comp_id, comp_name in hubspot_companies.items():
                                    if comp_name == matched_name:
                                        facility_hubspot_record_id = comp_id
                                        facility_hubspot_company = matched_name
                                        break
                    
                    # Get HubSpot contact info (individual contact)
                    hubspot_contact = hubspot_contacts.get(email, {})
                    
                    row['task_id'] = task_id
                    row['task_status'] = task_status
                    row['customer_type'] = customer_type
                    row['services'] = services
                    row['hubspot_url'] = hubspot_url
                    row['hubspot corporation record id'] = hubspot_corp_record_id
                    row['hubspot corporation name'] = hubspot_corp_name
                    row['facility_task_id'] = facility_task_id
                    row['facility_task_name'] = facility_task_name
                    row['facility_corporation_task'] = facility_corporation_task
                    row['facility_corporation_name'] = facility_corporation_name
                    row['facility_hubspot_url'] = facility_hubspot_url
                    row['facility_hubspot_record_id'] = facility_hubspot_record_id
                    row['facility_hubspot_company'] = facility_hubspot_company
                    row['hubspot_contact_id'] = hubspot_contact.get('contact_id', '')
                    row['hubspot_contact_first_name'] = hubspot_contact.get('first_name', '')
                    row['hubspot_contact_last_name'] = hubspot_contact.get('last_name', '')
                    row['match_method'] = method
                    
                    writer.writerow(row)
        
        logger.info(f"Step 3 complete.")
        logger.info(f"Rows Processed: {total_count}, Excluded: {excluded_count}")
        logger.info(f"Matches by Alias: {matched_alias}, Org Code: {matched_org}, Fuzzy: {matched_name_fuzzy}")
        logger.info(f"HubSpot Contacts matched: {len([e for e in all_emails if e in hubspot_contacts])}")
        logger.info(f"Output: {STEP3_OUTPUT}")
        return True
        
    except Exception as e:
        logger.error(f"Error processing CSV: {e}")
        import traceback
        traceback.print_exc()
        return False

# =============================================================================
# STEP 4: APPEND LOGIN DATA
# =============================================================================

def step4_append_login_data():
    """Step 4: Append login data from the login CSV."""
    logger.info("=" * 50)
    logger.info("STEP 4: Append Login Data")
    logger.info("=" * 50)
    
    if not os.path.exists(STEP3_OUTPUT):
        logger.error(f"Input file {STEP3_OUTPUT} not found. Run step 3 first.")
        return False
    
    if not os.path.exists(LOGIN_DATA_FILE):
        logger.error(f"Login data file {LOGIN_DATA_FILE} not found.")
        return False
    
    try:
        # Read login data and filter out "Total" rows
        login_data = {}
        with open(LOGIN_DATA_FILE, mode='r', encoding='utf-8-sig') as login_file:
            reader = csv.DictReader(login_file)
            for row in reader:
                username = row.get('Username', '').strip()
                # Skip "Total" rows
                if username and username.lower() != 'total':
                    login_data[username.lower()] = {
                        'count_of_views': row.get('Count of Views', ''),
                        'last_login': row.get('Last Login', '')
                    }
        
        logger.info(f"Loaded {len(login_data)} login records (excluding totals)")
        
        # Read enriched CSV and append login data
        with open(STEP3_OUTPUT, mode='r', newline='', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            fieldnames = list(reader.fieldnames)
            
            # Add login columns if not present
            if 'count_of_views' not in fieldnames:
                fieldnames.append('count_of_views')
            if 'last_login' not in fieldnames:
                fieldnames.append('last_login')
            
            with open(FINAL_OUTPUT, mode='w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()
                
                matched_logins = 0
                total_rows = 0
                
                # Count rows for progress
                row_list = list(reader)
                total_rows = len(row_list)
                
                for idx, row in enumerate(row_list):
                    # Progress logging every 500 rows
                    if idx > 0 and idx % 500 == 0:
                        logger.info(f"Step 4 progress: {idx}/{total_rows} rows processed")
                        outfile.flush()
                    
                    email = row.get('email', '').strip().lower()
                    
                    if email in login_data:
                        row['count_of_views'] = login_data[email]['count_of_views']
                        row['last_login'] = login_data[email]['last_login']
                        matched_logins += 1
                    else:
                        # Keep empty for non-matched users
                        row['count_of_views'] = ''
                        row['last_login'] = ''
                    
                    writer.writerow(row)
        
        logger.info(f"Step 4 complete.")
        logger.info(f"Total rows: {total_rows}, Matched with login data: {matched_logins}")
        logger.info(f"Output: {FINAL_OUTPUT}")
        return True
        
    except Exception as e:
        logger.error(f"Error appending login data: {e}")
        return False

# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='Email Processing Pipeline')
    parser.add_argument('--step', type=int, choices=[1, 2, 3, 4], 
                        help='Run specific step only')
    parser.add_argument('--skip-api', action='store_true',
                        help='Skip API-dependent steps (step 2 - email verification)')
    parser.add_argument('--skip-bq', action='store_true',
                        help='Skip BigQuery-dependent steps (3)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be done without executing')
    
    args = parser.parse_args()
    
    # Determine which steps to run
    steps_to_run = []
    if args.step:
        steps_to_run = [args.step]
    else:
        steps_to_run = [1, 2, 3, 4]
    
    if args.dry_run:
        logger.info("DRY RUN MODE - No changes will be made")
        logger.info(f"Steps to run: {steps_to_run}")
        logger.info(f"Skip API: {args.skip_api}, Skip BQ: {args.skip_bq}")
        return
    
    # Execute steps
    success = True
    
    if 1 in steps_to_run:
        if not step1_filter_rows():
            success = False
    
    if 2 in steps_to_run and success and not args.skip_api:
        if not step2_verify_emails():
            success = False
    
    if 3 in steps_to_run and success and not args.skip_bq:
        if not step3_enrich_csv():
            success = False
    
    if 4 in steps_to_run and success:
        if not step4_append_login_data():
            success = False
    
    if success:
        logger.info("=" * 50)
        logger.info("PIPELINE COMPLETE!")
        logger.info(f"Final output: {FINAL_OUTPUT}")
        logger.info("=" * 50)
    else:
        logger.error("Pipeline failed. Check logs for details.")
        sys.exit(1)

if __name__ == "__main__":
    main()
