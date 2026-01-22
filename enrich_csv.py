import csv
import json
import os
import difflib
import re
from google.cloud import bigquery

INPUT_FILE = 'cleaned view user list.csv'
OUTPUT_FILE = 'cleaned view user list_enriched.csv'
PROJECT_ID = 'gen-lang-client-0844868008'
LIST_ID = '901302721443'  # Corporations

# Corporations to explicitly exclude
EXCLUDED_CORPORATIONS = {'DATAIQ (DEMO)', 'DATA IQ (ORG)', 'DATAIQ (FPA TEST)'}

# Manual Aliases: CSV Code -> ClickUp Code
ORG_CODE_ALIASES = {
    'PHGUS': 'CCS',
    'PHCLA': 'PMHCC',
    'ALVCC': 'PCHM'
}

# Corporation Name Aliases: CSV Name -> ClickUp Name
CORP_NAME_ALIASES = {
    'Nava Healthcare / Arabella': 'Nava Healthcare',
    'PacifiCare Health Management (ALVCC)': 'PacifiCare Health Management',
    'Vivage': 'Vivage Management',
    'Judson Village': 'Hillstone'
}

def get_clickup_maps():
    """
    Fetches all tasks from the Corporations list and builds maps:
    1. org_map: org_code (cleaned) -> (task_id, task_status, customer_type, hubspot_url, hubspot_record_id, hubspot_company)
    2. name_list: List of tuples (original_name, normalized_name, task_id, task_status, customer_type, hubspot_url, hubspot_record_id, hubspot_company) for fuzzy matching
    3. task_map: task_id -> (customer_type, hubspot_url, hubspot_record_id, hubspot_company) for quick lookup
    """
    client = bigquery.Client(project=PROJECT_ID)

    # Fetch HubSpot companies for name resolution
    hubspot_companies = get_hubspot_companies()

    # Now get ClickUp data
    query = f"""
        SELECT
            id,
            name,
            status,
            custom_fields
        FROM
            `gen-lang-client-0844868008.ClickUp_AirbyteCustom.task`
        WHERE
            JSON_VALUE(list, '$.id') = '{LIST_ID}'
    """

    print("Fetching ClickUp tasks from BigQuery...")
    query_job = client.query(query)
    results = query_job.result()

    org_map = {}
    name_list = [] # Using a list for iteration instead of dict for exact lookup
    task_map = {} # task_id -> (customer_type, hubspot_url, hubspot_company)

    for row in results:
        task_id = row.id
        task_name = row.name
        status_json = row.status

        # Parse status
        status_val = 'unknown'
        if isinstance(status_json, str):
            try:
                s_dict = json.loads(status_json)
                status_val = s_dict.get('status')
            except:
                pass
        elif isinstance(status_json, dict):
            status_val = status_json.get('status')

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

        # Extract Hubspot URL
        hubspot_url = ''
        hubspot_field = next((f for f in cfields if f.get('name') == 'Hubspot URL'), None)
        if hubspot_field:
            hubspot_url = hubspot_field.get('value', '')
            if isinstance(hubspot_url, list) and hubspot_url:
                hubspot_url = hubspot_url[0]  # Take first URL if it's a list

        # Extract HubSpot record ID and company name from URL
        hubspot_record_id = ''
        hubspot_company = ''
        if hubspot_url:
            # Extract record ID from URL (last part after the last /)
            url_parts = hubspot_url.rstrip('/').split('/')
            if url_parts:
                hubspot_record_id = url_parts[-1]
                hubspot_company = hubspot_companies.get(hubspot_record_id, '')

        # Store task info with customer type, hubspot url, record id, and hubspot company
        task_info = (task_id, status_val, customer_type, hubspot_url, hubspot_record_id, hubspot_company)
        task_map[task_id] = (customer_type, hubspot_url, hubspot_record_id, hubspot_company)

        # Populate Name List (normalized)
        if task_name:
            norm_name = task_name.strip().upper()
            original_name = task_name.strip()
            name_list.append((original_name, norm_name, task_id, status_val, customer_type, hubspot_url, hubspot_record_id, hubspot_company))

        # Find Org Code field
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

    print(f"Built HubSpot company mapping: {len(hubspot_companies)} companies")
    return org_map, name_list, task_map

def get_hubspot_companies():
    """
    Fetches HubSpot companies from BigQuery and returns a dict mapping id to name.
    """
    client = bigquery.Client(project=PROJECT_ID)

    query = f"""
        SELECT
            id,
            properties_name
        FROM
            `gen-lang-client-0844868008.HubSpot_Airbyte.companies`
        WHERE
            properties_name IS NOT NULL
    """

    print("Fetching HubSpot companies from BigQuery...")
    query_job = client.query(query)
    results = query_job.result()

    hubspot_map = {}
    for row in results:
        hubspot_map[str(row.id)] = row.properties_name.strip() if row.properties_name else ''

    print(f"Fetched {len(hubspot_map)} HubSpot companies.")
    return hubspot_map

def enrich_csv():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        return

    # 1. Build the maps
    org_map, name_list, task_map = get_clickup_maps()
    hubspot_companies = get_hubspot_companies()
    print(f"Built lookup data: {len(org_map)} org codes, {len(name_list)} task names, {len(hubspot_companies)} HubSpot companies.")

    # 2. Process CSV
    try:
        with open(INPUT_FILE, mode='r', newline='', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            fieldnames = reader.fieldnames

            # Ensure new columns are in fieldnames if not already
            if 'task_id' not in fieldnames:
                fieldnames.append('task_id')
            if 'task_status' not in fieldnames:
                fieldnames.append('task_status')
            if 'customer_type' not in fieldnames:
                fieldnames.append('customer_type')
            if 'hubspot_url' not in fieldnames:
                fieldnames.append('hubspot_url')
            if 'hubspot corporation record id' not in fieldnames:
                fieldnames.append('hubspot corporation record id')
            if 'hubspot corporation name' not in fieldnames:
                fieldnames.append('hubspot corporation name')
            if 'facility_task_id' not in fieldnames:
                fieldnames.append('facility_task_id')
            if 'facility_task_name' not in fieldnames:
                fieldnames.append('facility_task_name')
            if 'facility_corporation_task' not in fieldnames:
                fieldnames.append('facility_corporation_task')
            if 'facility_corporation_name' not in fieldnames:
                fieldnames.append('facility_corporation_name')
            if 'facility_hubspot_url' not in fieldnames:
                fieldnames.append('facility_hubspot_url')
            if 'facility_hubspot_record_id' not in fieldnames:
                fieldnames.append('facility_hubspot_record_id')
            if 'facility_hubspot_company' not in fieldnames:
                fieldnames.append('facility_hubspot_company')
            if 'contact_record_id' not in fieldnames:
                fieldnames.append('contact_record_id')
            if 'contact_company' not in fieldnames:
                fieldnames.append('contact_company')
            if 'match_method' not in fieldnames:
                fieldnames.append('match_method')
            if 'job title' not in fieldnames:
                fieldnames.append('job title')

            with open(OUTPUT_FILE, mode='w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()

                matched_org = 0
                matched_name_fuzzy = 0
                matched_alias = 0
                total_count = 0
                excluded_count = 0

                for row in reader:
                    # Extract job title from last_name if it contains parentheses
                    last_name = row.get('last_name', '')
                    job_match = re.search(r'\((.*?)\)', last_name)
                    if job_match:
                        row['job title'] = job_match.group(1)
                        # Remove the parentheses and content from last_name
                        row['last_name'] = re.sub(r'\(.*?\)', '', last_name).strip()
                    else:
                        row['job title'] = ''

                    covr_corp = row.get('covr_corporation', '').strip()

                    # 0. Filter
                    if covr_corp.upper() in [ex.upper() for ex in EXCLUDED_CORPORATIONS]:
                        excluded_count += 1
                        continue # Skip this row

                    total_count += 1
                    user_type = row.get('View User type', '').strip()

                    # Skip matching for certain user types
                    if user_type not in ['QRM Labor - facility', 'View Labor - facility', 'View Labor - Corp', 'QRM Labor - Corp']:
                        # Set all matching fields to empty
                        row['task_id'] = ''
                        row['task_status'] = ''
                        row['customer_type'] = ''
                        row['hubspot_url'] = ''
                        row['hubspot corporation record id'] = ''
                        row['hubspot corporation name'] = ''
                        row['facility_task_id'] = ''
                        row['facility_task_name'] = ''
                        row['facility_corporation_task'] = ''
                        row['facility_corporation_name'] = ''
                        row['facility_hubspot_url'] = ''
                        row['facility_hubspot_record_id'] = ''
                        row['facility_hubspot_company'] = ''
                        row['contact_record_id'] = ''
                        row['contact_company'] = ''
                        row['match_method'] = ''
                        writer.writerow(row)
                        continue

                    org_code = row.get('org_code', '').strip().upper()
                    norm_covr_corp = covr_corp.upper()

                    task_id = ''
                    task_status = ''
                    customer_type = ''
                    hubspot_url = ''
                    hubspot_corp_record_id = ''
                    hubspot_corp_name = ''
                    method = ''

                    # 1. Try Manual Alias
                    if not task_id and org_code in ORG_CODE_ALIASES:
                        alias_target = ORG_CODE_ALIASES[org_code]
                        task_info = org_map.get(alias_target)
                        if task_info:
                            task_id, task_status, customer_type, hubspot_url, hubspot_corp_record_id, hubspot_corp_name = task_info
                            method = f'alias({alias_target})'
                            matched_alias += 1

                    # 2. Try Org Code (Standard)
                    if not task_id and org_code:
                        task_info = org_map.get(org_code)
                        if task_info:
                            task_id, task_status, customer_type, hubspot_url, hubspot_corp_record_id, hubspot_corp_name = task_info
                            method = 'org_code'
                            matched_org += 1

                    # 3. Try Corporation Name Alias
                    if not task_id and norm_covr_corp in CORP_NAME_ALIASES:
                        alias_name = CORP_NAME_ALIASES[norm_covr_corp].upper()
                        # Look for exact match with the alias
                        for cu_orig_name, cu_norm_name, cu_id, cu_status, cu_customer_type, cu_hubspot_url, cu_hubspot_corp_record_id, cu_hubspot_corp_name in name_list:
                            if cu_norm_name == alias_name:
                                task_id = cu_id
                                task_status = cu_status
                                customer_type = cu_customer_type
                                hubspot_url = cu_hubspot_url
                                hubspot_corp_record_id = cu_hubspot_corp_record_id
                                hubspot_corp_name = cu_hubspot_corp_name
                                method = f'name_alias({alias_name})'
                                matched_name_fuzzy += 1  # Count as fuzzy for now
                                break


                    # 4. Fallback to Fuzzy Name Match
                    if not task_id and norm_covr_corp:
                        # Find best match? Or first match?
                        # We look for mutual containment: A in B OR B in A
                        # Iterating through all ClickUp task names
                        for cu_orig_name, cu_norm_name, cu_id, cu_status, cu_customer_type, cu_hubspot_url, cu_hubspot_corp_record_id, cu_hubspot_corp_name in name_list:
                            # Strict containment prevents some false positives but allows "WeCare" <-> "WeCare Health..."
                            if (norm_covr_corp in cu_norm_name) or (cu_norm_name in norm_covr_corp):
                                task_id = cu_id
                                task_status = cu_status
                                customer_type = cu_customer_type
                                hubspot_url = cu_hubspot_url
                                hubspot_corp_record_id = cu_hubspot_corp_record_id
                                hubspot_corp_name = cu_hubspot_corp_name
                                method = 'name_fuzzy_match'
                                matched_name_fuzzy += 1
                                break # Stop after first match

                    # Facility matching for single facility entries (only for facility user types)
                    facility_task_id = ''
                    facility_task_name = ''
                    facility_corporation_task = ''
                    facility_corporation_name = ''
                    facility_hubspot_url = ''
                    facility_hubspot_record_id = ''
                    facility_hubspot_company = ''

                    facilities = row.get('facilities', '').strip()
                    if user_type in ['QRM Labor - facility', 'View Labor - facility'] and facilities and ',' not in facilities:  # Single facility only
                        # Perform fuzzy name matching against ClickUp company list
                        norm_facility = facilities.upper()
                        for cu_orig_name, cu_norm_name, cu_id, cu_status, cu_customer_type, cu_hubspot_url, cu_hubspot_record_id, cu_hubspot_company in name_list:
                            # Strict containment prevents some false positives but allows matching
                            if (norm_facility in cu_norm_name) or (cu_norm_name in norm_facility):
                                facility_task_id = cu_id
                                facility_task_name = cu_orig_name  # Use the original task name from ClickUp
                                facility_hubspot_url = cu_hubspot_url
                                facility_hubspot_record_id = cu_hubspot_record_id
                                facility_hubspot_company = cu_hubspot_company
                                # Try to find the corporation task by looking up the facility's org code or corporation
                                # For now, we'll use the already matched corporation task_id if available
                                facility_corporation_task = task_id if task_id else ''
                                break

                        # Look up the corporation name if we have a corporation task ID
                        if facility_corporation_task:
                            for cu_orig_name, cu_norm_name, cu_id, cu_status, cu_customer_type, cu_hubspot_url, cu_hubspot_record_id, cu_hubspot_company in name_list:
                                if cu_id == facility_corporation_task:
                                    facility_corporation_name = cu_orig_name
                                    break

                    # HubSpot Contact Lookup - REMOVED as requested
                    contact_record_id = ''
                    contact_company = ''

                    # HubSpot Company Lookup for Facility Names (for labor facility types)
                    if user_type in ['QRM Labor - facility', 'View Labor - facility']:
                        facility_name = row.get('facilities', '').strip()
                        if facility_name:
                            # Use hubspot_map (the dict) for faster lookup if possible, but keep fuzzy for now
                            # Actually hubspot_companies is still a list in the script context? No, wait.
                            # In enrich_csv(), hubspot_companies = get_hubspot_companies() which now returns a dict.
                            # I need to fix how get_close_matches is called.
                            hubspot_names = list(hubspot_companies.values())
                            matches = difflib.get_close_matches(facility_name, hubspot_names, n=1, cutoff=0.6)
                            if matches:
                                matched_name = matches[0]
                                # Find ID by value
                                for comp_id, comp_name in hubspot_companies.items():
                                    if comp_name == matched_name:
                                        facility_hubspot_record_id = comp_id
                                        facility_hubspot_company = matched_name
                                        break

                    row['task_id'] = task_id
                    row['task_status'] = task_status
                    row['customer_type'] = customer_type
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
                    row['contact_record_id'] = contact_record_id
                    row['contact_company'] = contact_company
                    row['match_method'] = method

                    writer.writerow(row)
                    
        print(f"Enrichment complete.")
        print(f"Rows Processed: {total_count}")
        print(f"Rows Excluded (filtered): {excluded_count}")
        print(f"Matches by Alias: {matched_alias}")
        print(f"Matches by Org Code: {matched_org}")
        print(f"Matches by Name (Fuzzy): {matched_name_fuzzy}")
        print(f"Total Matches: {matched_alias + matched_org + matched_name_fuzzy}")
        print(f"Output saved to: {OUTPUT_FILE}")
        
    except Exception as e:
        print(f"Error processing CSV: {e}")

if __name__ == "__main__":
    enrich_csv()
