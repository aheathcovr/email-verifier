#!/usr/bin/env python3
"""
Comprehensive Email Processing Pipeline

This script processes email data through 4 stages:
1. Email Verification - Validates emails using local API
2. Filtering - Filters by org_code and classifies View User types
3. Enrichment - Adds ClickUp and HubSpot data from BigQuery
4. Login Data - Appends last login and login count data

Usage:
    python process_pipeline.py                    # Run all steps
    python process_pipeline.py --step 1         # Run only step 1 (email verification)
    python process_pipeline.py --step 2         # Run only step 2 (filtering)
    python process_pipeline.py --step 3         # Run only step 3 (enrichment)
    python process_pipeline.py --step 4         # Run only step 4 (login data)
    python process_pipeline.py --skip-api        # Skip API-dependent steps
    python process_pipeline.py --skip-bq         # Skip BigQuery-dependent steps
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

# Exclusion lists (from filter_emails.py)
EXCLUDED_ORG_CODES = {
    "CCS","GCLTC","RHG","SLTC","VOLH","MSTG","VOLH / MSTG","CSC","AVIANA", "SLTC", "PCHC", "BONA", "CORHP", "CSNHC", "DHM", "HILLHG",
    "NEX", "OVHC", "EVEREST", "AVALA", "ELVT", "ECARE", "ELEVHC", "KEREM",
    "MHCS", "ASCEND", "FCC2", "SSS", "TBHS", "WESH", "LOH", "AVAN", "ENG"
}

VIEW_LABOR_ORG_CODES = {
    "ASPIRE", "CARA", "CSJI", "FUNDAMENTAL", "HMGS", "JACO", "PMHCC", "PMHCCA", "RUBY"
}

QRM_LABOR_ORG_CODES = {
    "ASHFORD", "BLR", "BRIGHTS", "CAS", "CCYOU", "DHC", "EHMG", "FPACP", "GEMINI", "TGR", "VOLH"
}

CHURNED_VIEW_CORP_ORG_CODES = {
    "CONQ", "EXCEL", "ABBEY", "BEHC", "ARI", "AristaRecovery", "NHG", "PEAC", "PRIME", "KEY", "WEC", "CMG", "RHHR", "DIA", "CAP", "PGC", "EAC", "ANEW", "HCG", "AVHS", "SMTHC", "CWR", "AWAR", "SSH", "BRISTOL", "IRCC", "MWDNC", "SSN", "CHPK", "BHCG"
}

# Enrichment Configuration (from enrich_csv.py)
EXCLUDED_CORPORATIONS = {'DATAIQ (DEMO)', 'DATA IQ (ORG)', 'DATAIQ (FPA TEST)'}
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
    
    Returns:
        - view_clinical_orgs: set of org codes for View Clinical
        - qrm_cadence_orgs: set of org codes for QRM Cadence
        - other_active_orgs: set of org codes for Other Active
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
    other_active_orgs = set()    # View + active/implementation
    
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
        
        # Only process View customer types
        if 'view' not in customer_type:
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
        is_active = status_val == 'active'
        is_implementation = status_val == 'implementation'
        
        for org_code in org_codes:
            # QRM Cadence: View + QRM + NOT Labor + active
            if has_qrm and not has_labor and is_active:
                qrm_cadence_orgs.add(org_code)
            # View Clinical: View + NOT Labor + NOT QRM + active
            elif not has_labor and not has_qrm and is_active:
                view_clinical_orgs.add(org_code)
            # Other Active: View + active OR implementation
            elif is_active or is_implementation:
                other_active_orgs.add(org_code)
    
    logger.info(f"Found {len(view_clinical_orgs)} View Clinical org codes: {list(view_clinical_orgs)[:10]}...")
    logger.info(f"Found {len(qrm_cadence_orgs)} QRM Cadence org codes: {list(qrm_cadence_orgs)[:10]}...")
    logger.info(f"Found {len(other_active_orgs)} Other Active org codes: {list(other_active_orgs)[:10]}...")
    
    return view_clinical_orgs, qrm_cadence_orgs, other_active_orgs

# =============================================================================
# STEP 1: EMAIL VERIFICATION
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

def step1_verify_emails():
    """Step 1: Verify emails using local API."""
    logger.info("=" * 50)
    logger.info("STEP 1: Email Verification")
    logger.info("=" * 50)
    
    if not os.path.exists(INPUT_FILE):
        logger.error(f"Input file {INPUT_FILE} not found.")
        return False
    
    # Read all rows
    rows = []
    email_col = None
    
    try:
        with open(INPUT_FILE, mode='r', encoding='utf-8-sig') as csvfile:
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
    
    with open(STEP1_OUTPUT, mode='w', newline='', encoding='utf-8') as outfile:
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
    
    logger.info(f"Step 1 complete. Output: {STEP1_OUTPUT}")
    return True

# =============================================================================
# STEP 2: FILTERING
# =============================================================================

def step2_filter_emails(use_dynamic_org_codes=True):
    """Step 2: Filter by org_code and classify View User types and Campaign.
    
    Args:
        use_dynamic_org_codes: If True, query ClickUp to dynamically determine
            org code categories. If False, use hardcoded sets.
    """
    logger.info("=" * 50)
    logger.info("STEP 2: Filtering")
    logger.info("=" * 50)
    
    if not os.path.exists(STEP1_OUTPUT):
        logger.error(f"Input file {STEP1_OUTPUT} not found. Run step 1 first.")
        return False
    
    # Get org codes (either dynamic from ClickUp or hardcoded)
    if use_dynamic_org_codes:
        logger.info("Fetching org codes dynamically from ClickUp...")
        view_clinical_codes, qrm_cadence_codes, other_active_codes = get_org_codes_from_clickup()
    else:
        logger.info("Using hardcoded org code sets...")
        view_clinical_codes = VIEW_LABOR_ORG_CODES
        qrm_cadence_codes = QRM_LABOR_ORG_CODES
        other_active_codes = CHURNED_VIEW_CORP_ORG_CODES
    
    try:
        with open(STEP1_OUTPUT, mode='r', newline='', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            
            if 'org_code' not in reader.fieldnames:
                logger.error(f"'org_code' column not found in {STEP1_OUTPUT}")
                return False
            
            # Add both View User type and Campaign columns
            fieldnames = list(reader.fieldnames) + ['View User type', 'campaign']
            
            with open(STEP2_OUTPUT, mode='w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()
                
                count_kept = 0
                count_removed = 0
                
                for row in reader:
                    org_code = row.get('org_code', '').strip()
                    facilities = row.get('facilities', '').strip()
                    
                    # Determine campaign category
                    campaign = ''
                    if org_code in view_clinical_codes:
                        campaign = "View Clinical"
                    elif org_code in qrm_cadence_codes:
                        campaign = "QRM Cadence"
                    elif org_code in other_active_codes:
                        campaign = "Other Active"
                    
                    # Determine View User type (facility vs corp)
                    if campaign:
                        if not facilities or ',' in facilities:
                            view_user_type = f"{campaign} - Corp"
                        else:
                            view_user_type = f"{campaign} - facility"
                    else:
                        view_user_type = "View - No Labor"
                    
                    row['View User type'] = view_user_type
                    row['campaign'] = campaign
                    writer.writerow(row)
                    count_kept += 1
        
        logger.info(f"Step 2 complete. Rows processed: {count_kept}")
        logger.info(f"Output: {STEP2_OUTPUT}")
        return True
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return False

# =============================================================================
# STEP 3: ENRICHMENT (BIGQUERY)
# =============================================================================

def get_clickup_maps():
    """Fetch ClickUp tasks and build lookup maps."""
    client = bigquery.Client(project=BQ_PROJECT_ID)
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


def get_hubspot_contacts(emails):
    """
    Fetch HubSpot contacts from BigQuery by email addresses.
    Returns contact information for individual people.
    """
    if not emails:
        return {}
    
    client = bigquery.Client(project=BQ_PROJECT_ID)
    
    # Batch emails to avoid query size limits (max 10000 bytes per query)
    email_list = list(emails)
    hubspot_contacts = {}
    
    # Process in batches of 100 emails
    batch_size = 100
    for i in range(0, len(email_list), batch_size):
        batch = email_list[i:i+batch_size]
        email_conditions = ' OR '.join([f"properties_email = '{email}'" for email in batch if email])
        
        query = f"""
            SELECT
                properties_email as email,
                id,
                properties_firstname as first_name,
                properties_lastname as last_name
            FROM
                `gen-lang-client-0844868008.HubSpot_Airbyte.contacts`
            WHERE
                {email_conditions}
        """
        
        try:
            query_job = client.query(query)
            results = query_job.result()
            
            for row in results:
                email = row.email if hasattr(row, 'email') else ''
                if email:
                    hubspot_contacts[email.lower()] = {
                        'contact_id': row.id if hasattr(row, 'id') else '',
                        'first_name': row.first_name if hasattr(row, 'first_name') else '',
                        'last_name': row.last_name if hasattr(row, 'last_name') else ''
                    }
        except Exception as e:
            logger.warning(f"Error querying HubSpot contacts batch {i//batch_size + 1}: {e}")
    
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
        
        # Fetch HubSpot contacts by email
        hubspot_contacts = get_hubspot_contacts(all_emails)
        
        # Get ClickUp and HubSpot Company data
        org_map, name_list, task_map = get_clickup_maps()
        hubspot_companies = get_hubspot_companies()
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
                
                for row in reader:
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
                
                for row in reader:
                    email = row.get('email', '').strip().lower()
                    total_rows += 1
                    
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
                        help='Skip API-dependent steps (1)')
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
    
    if 1 in steps_to_run and not args.skip_api:
        if not step1_verify_emails():
            success = False
    
    if 2 in steps_to_run and success:
        if not step2_filter_emails():
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
