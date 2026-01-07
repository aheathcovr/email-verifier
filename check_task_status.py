import argparse
import json
import sys
from google.cloud import bigquery

def get_clickup_task_status(org_code):
    """
    Queries BigQuery for ClickUp tasks in the 'Corporations' list and finds 
    the task matching the provided org_code.
    """
    
    project_id = 'gen-lang-client-0844868008'
    client = bigquery.Client(project=project_id)

    # We want tasks from the 'Corporations' list. 
    # List ID found in previous steps: 901302721443
    # ClickUp_AirbyteCustom.task table filter
    
    query = """
        SELECT 
            id, 
            status, 
            custom_fields 
        FROM 
            `gen-lang-client-0844868008.ClickUp_AirbyteCustom.task`
        WHERE 
            JSON_VALUE(list, '$.id') = '901302721443'
    """

    print(f"Querying BigQuery project {project_id}...")
    try:
        query_job = client.query(query)
        results = query_job.result()  # Waits for job to complete.
    except Exception as e:
        print(f"Error executing query: {e}")
        return

    print("Processing results...")
    
    found_match = False

    for row in results:
        task_id = row.id
        status_json = row.status
        
        status_dict = {}
        # Handle cases where the library might return parsed dict or raw string
        if isinstance(status_json, str):
            try:
                status_dict = json.loads(status_json)
            except:
                pass
        elif isinstance(status_json, dict):
            status_dict = status_json
            
        task_status = status_dict.get('status', 'unknown')

        # Custom fields handling
        custom_fields_raw = row.custom_fields
        custom_fields = []
        
        if isinstance(custom_fields_raw, str):
            try:
                custom_fields = json.loads(custom_fields_raw)
            except:
                continue
        elif isinstance(custom_fields_raw, list):
            custom_fields = custom_fields_raw
        
        # Look for "Org Code" field
        org_code_field = next((f for f in custom_fields if f.get('name') == 'Org Code'), None)
        
        if not org_code_field:
            continue

        # Extract selected values (IDs)
        selected_value_ids = org_code_field.get('value', [])
        
        if isinstance(selected_value_ids, str):
            selected_value_ids = [selected_value_ids]
            
        if not selected_value_ids:
            continue

        # Get options to map IDs to Labels
        type_config = org_code_field.get('type_config', {})
        options = type_config.get('options', [])
        
        # Check for match
        for opt in options:
            if opt.get('id') in selected_value_ids:
                label = opt.get('label', '')
                
                # Check for exact word match to avoid partial substrings (e.g. SLTC inside SLTCC)
                # Replacing standard delimiters with space
                normalized_label = label.replace(',', ' ').replace('-', ' ').replace('/', ' ')
                words = normalized_label.split()
                
                if org_code.upper() in [w.upper() for w in words]:
                    print(f"Match found!")
                    print(f"Org Code: {org_code}")
                    print(f"Task ID: {task_id}")
                    print(f"Task Status: {task_status}")
                    print(f"Full Label: {label}")
                    print("-" * 30)
                    found_match = True
    
    if not found_match:
        print(f"No task found with Org Code: {org_code}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Check ClickUp task status for a given Org Code.')
    parser.add_argument('org_code', help='The organizational code to search for (e.g., AVIANA)')
    args = parser.parse_args()

    get_clickup_task_status(args.org_code)
