#!/usr/bin/env python3
"""
Script to find matching services, customer type, and task name for ClickUp tasks
based on task_id values from the CSV file.
"""

import csv
import sys
import json
from collections import defaultdict
import argparse
import os
from google.cloud import bigquery

def get_hubspot_contact_info(emails):
    """
    Queries BigQuery for HubSpot contacts and returns contact information
    for the given email addresses.
    """
    if not emails:
        return {}

    project_id = 'gen-lang-client-0844868008'
    client = bigquery.Client(project=project_id)

    # Create a list of email conditions for the query
    email_conditions = ' OR '.join([f"properties_email = '{email}'" for email in emails if email])

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

    print(f"Querying BigQuery for HubSpot contact information...")
    try:
        query_job = client.query(query)
        results = query_job.result()  # Waits for job to complete.
    except Exception as e:
        print(f"Error executing HubSpot BigQuery: {e}")
        return {}

    hubspot_contact_info = {}

    for row in results:
        email = row.email if hasattr(row, 'email') else ''
        contact_id = row.id if hasattr(row, 'id') else ''
        first_name = row.first_name if hasattr(row, 'first_name') else ''
        last_name = row.last_name if hasattr(row, 'last_name') else ''

        if email:
            hubspot_contact_info[email] = {
                'contact_id': contact_id,
                'first_name': first_name,
                'last_name': last_name
            }

    return hubspot_contact_info

def get_clickup_task_info(task_ids):
    """
    Queries BigQuery for ClickUp tasks and extracts customer_type, services, and task name
    for the given task_ids. Resolves IDs to human-readable names.
    """
    if not task_ids:
        return {}

    project_id = 'gen-lang-client-0844868008'
    client = bigquery.Client(project=project_id)

    # Query ClickUp tasks from the 'Corporations' list
    query = """
        SELECT
            id,
            name,
            custom_fields
        FROM
            `gen-lang-client-0844868008.ClickUp_AirbyteCustom.task`
        WHERE
            JSON_VALUE(list, '$.id') = '901302721443'
    """

    print(f"Querying BigQuery for ClickUp task information...")
    try:
        query_job = client.query(query)
        results = query_job.result()  # Waits for job to complete.
    except Exception as e:
        print(f"Error executing BigQuery: {e}")
        return {}

    clickup_task_info = {}

    for row in results:
        task_id = row.id
        if task_id not in task_ids:
            continue

        task_name = row.name if hasattr(row, 'name') else ''

        # Parse custom fields
        custom_fields_raw = row.custom_fields
        custom_fields = []

        if isinstance(custom_fields_raw, str):
            try:
                custom_fields = json.loads(custom_fields_raw)
            except:
                continue
        elif isinstance(custom_fields_raw, list):
            custom_fields = custom_fields_raw

        # Extract customer_type and services from custom fields with ID resolution
        customer_type = ''
        services = ''

        for field in custom_fields:
            field_name = field.get('name', '')
            field_value = field.get('value', '')
            type_config = field.get('type_config', {})

            if field_name == 'Customer Type' and field_value:
                # Resolve customer type - could be numeric index or UUID
                options = type_config.get('options', [])

                if isinstance(field_value, (int, str)) and str(field_value).isdigit():
                    # Numeric value - likely corresponds to orderindex
                    field_index = int(field_value)
                    for option in options:
                        if option.get('orderindex') == field_index:
                            customer_type = option.get('name', field_value)
                            break
                elif isinstance(field_value, str) and options:
                    # String UUID - find matching option
                    for option in options:
                        if option.get('id') == field_value:
                            customer_type = option.get('name', field_value)
                            break
                else:
                    customer_type = str(field_value)

            elif field_name == 'Services' and field_value:
                # Resolve service IDs to names
                if isinstance(field_value, list) and type_config.get('options'):
                    service_names = []
                    for service_id in field_value:
                        # Find the option with matching ID
                        for option in type_config.get('options', []):
                            if option.get('id') == service_id:
                                service_names.append(option.get('label', service_id))
                                break
                    services = ', '.join(service_names) if service_names else str(field_value)
                elif isinstance(field_value, list):
                    services = ', '.join(field_value)
                else:
                    services = str(field_value)

        clickup_task_info[task_id] = {
            'task_name': task_name,
            'customer_type': customer_type,
            'services': services
        }

    return clickup_task_info

def load_csv_data(file_path):
    """Load CSV data and organize by task_id, including ALL contacts"""
    task_data = defaultdict(list)  # Changed to list to handle multiple entries per task_id

    with open(file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            task_id = row.get('task_id', '').strip()
            if not task_id:
                # Create a unique task_id for rows without one
                task_id = f"no_task_id_{len(task_data)}"

            # Store relevant information for each task_id
            task_data[task_id].append({
                'services': row.get('facilities', '').strip() if row.get('facilities') else '',
                'customer_type': row.get('customer_type', '').strip() if row.get('customer_type') else '',
                'task_name': row.get('facility_task_name', '').strip() if row.get('facility_task_name') else '',
                'facility_corporation_task': row.get('facility_corporation_task', '').strip() if row.get('facility_corporation_task') else '',
                'facility_corporation_name': row.get('facility_corporation_name', '').strip() if row.get('facility_corporation_name') else '',
                'email': row.get('email', '').strip() if row.get('email') else '',
                'first_name': row.get('first_name', '').strip() if row.get('first_name') else '',
                'last_name': row.get('last_name', '').strip() if row.get('last_name') else '',
                'title': row.get('Title', '').strip() if row.get('Title') else '',
                'hubspot_company': row.get('hubspot_company', '').strip() if row.get('hubspot_company') else '',
                'hubspot_record_id': row.get('hubspot_record_id', '').strip() if row.get('hubspot_record_id') else '',
                # Additional columns requested by user
                'hubspot_url': row.get('hubspot_url', '').strip() if row.get('hubspot_url') else '',
                'covr_corporation': row.get('covr_corporation', '').strip() if row.get('covr_corporation') else '',
                'org_code': row.get('org_code', '').strip() if row.get('org_code') else '',
                'role': row.get('role', '').strip() if row.get('role') else ''
            })

    return task_data

def find_task_info(task_data, task_ids, use_bigquery=False):
    """Find and display task information for given task_ids"""
    results = []

    # Get ClickUp task info from BigQuery if requested
    clickup_info = {}
    if use_bigquery:
        clickup_info = get_clickup_task_info(task_ids)

    # Collect all unique emails for HubSpot lookup
    all_emails = set()
    for task_id in task_ids:
        task_id = task_id.strip()
        if task_id in task_data:
            for entry in task_data[task_id]:
                if entry['email']:
                    all_emails.add(entry['email'])

    # Get HubSpot contact info for all emails
    hubspot_contact_info = {}
    if all_emails:
        hubspot_contact_info = get_hubspot_contact_info(all_emails)

    for task_id in task_ids:
        task_id = task_id.strip()
        if task_id in task_data:
            # Handle multiple entries for the same task_id
            for entry in task_data[task_id]:
                # Use ClickUp data if available, otherwise use CSV data
                clickup_data = clickup_info.get(task_id, {})

                services_value = clickup_data.get('services', entry['services'])
                email = entry['email']

                # Get HubSpot contact info from BigQuery
                hubspot_info = hubspot_contact_info.get(email, {})
                hubspot_contact_id = hubspot_info.get('contact_id', '')  # Individual contact ID from HubSpot
                hubspot_first_name = hubspot_info.get('first_name', '')
                hubspot_last_name = hubspot_info.get('last_name', '')

                result_entry = {
                    'task_id': task_id,
                    'services': services_value,
                    'customer_type': clickup_data.get('customer_type', entry['customer_type']),
                    'task_name': clickup_data.get('task_name', entry['task_name']),
                    'facility_corporation_task': entry['facility_corporation_task'],
                    'facility_corporation_name': entry['facility_corporation_name'],
                    'email': email,
                    'name': f"{entry['first_name']} {entry['last_name']}".strip(),
                    'title': entry['title'],
                    'company': entry['hubspot_company'],
                    'source': 'BigQuery' if task_id in clickup_info else 'CSV',
                    'has_qrm': 'YES' if services_value and 'QRM' in services_value else 'NO',
                    'hubspot_contact_id': hubspot_contact_id,
                    'hubspot_first_name': hubspot_first_name,
                    'hubspot_last_name': hubspot_last_name,
                    # Additional columns from original CSV
                    'hubspot_url': entry['hubspot_url'],
                    'hubspot_record_id': entry['hubspot_record_id'],
                    'covr_corporation': entry['covr_corporation'],
                    'org_code': entry['org_code'],
                    'role': entry['role']
                }
                results.append(result_entry)
        else:
            results.append({
                'task_id': task_id,
                'error': f'Task ID {task_id} not found in CSV'
            })

    return results

def display_results(results, output_csv=None):
    """Display the results in a readable format and optionally save to CSV"""
    if output_csv:
        # Write results to CSV file
        with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['task_id', 'name', 'email', 'title', 'company', 'customer_type',
                         'services', 'task_name', 'facility_corporation_task', 'facility_corporation_name', 'source', 'has_qrm', 'hubspot_contact_id', 'hubspot_first_name', 'hubspot_last_name',
                         'hubspot_url', 'hubspot_record_id', 'covr_corporation', 'org_code', 'role']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for result in results:
                if 'error' in result:
                    writer.writerow({
                        'task_id': result['task_id'],
                        'name': '',
                        'email': '',
                        'title': '',
                        'company': '',
                        'customer_type': '',
                        'services': '',
                        'task_name': '',
                        'facility_corporation_task': '',
                        'facility_corporation_name': '',
                        'source': '',
                        'has_qrm': '',
                        'hubspot_contact_id': '',
                        'hubspot_first_name': '',
                        'hubspot_last_name': '',
                        'error': result['error']
                    })
                else:
                    writer.writerow({
                        'task_id': result['task_id'],
                        'name': result['name'],
                        'email': result['email'],
                        'title': result['title'],
                        'company': result['company'],
                        'customer_type': result['customer_type'],
                        'services': result['services'],
                        'task_name': result['task_name'],
                        'facility_corporation_task': result['facility_corporation_task'],
                        'facility_corporation_name': result['facility_corporation_name'],
                        'source': result.get('source', ''),
                        'has_qrm': result.get('has_qrm', ''),
                        'hubspot_contact_id': result.get('hubspot_contact_id', ''),
                        'hubspot_first_name': result.get('hubspot_first_name', ''),
                        'hubspot_last_name': result.get('hubspot_last_name', ''),
                        'hubspot_url': result.get('hubspot_url', ''),
                        'hubspot_record_id': result.get('hubspot_record_id', ''),
                        'covr_corporation': result.get('covr_corporation', ''),
                        'org_code': result.get('org_code', ''),
                        'role': result.get('role', '')
                    })

        print(f"Results saved to: {output_csv}")

    # Display results to console
    for result in results:
        print(f"\nTask ID: {result['task_id']}")

        if 'error' in result:
            print(f"  Error: {result['error']}")
            continue

        print(f"  Name: {result['name']}")
        print(f"  Email: {result['email']}")
        print(f"  Title: {result['title']}")
        print(f"  Company: {result['company']}")
        print(f"  Customer Type: {result['customer_type']}")
        print(f"  Source: {result.get('source', 'CSV')}")

        # Services (facilities)
        services = result['services']
        if services:
            print(f"  Services: {services}")
        else:
            print("  Services: Not specified")

        # Task name
        task_name = result['task_name']
        if task_name:
            print(f"  Task Name: {task_name}")
        else:
            print("  Task Name: Not specified")

        # Facility corporation info
        if result['facility_corporation_task']:
            print(f"  Facility Corporation Task: {result['facility_corporation_task']}")
        if result['facility_corporation_name']:
            print(f"  Facility Corporation Name: {result['facility_corporation_name']}")

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Find ClickUp task information from CSV file')
    parser.add_argument('csv_file', help='Path to the CSV file')
    parser.add_argument('task_ids', nargs='*', help='Task IDs to search for (optional)')
    parser.add_argument('--output', '-o', help='Output CSV file path')
    parser.add_argument('--bigquery', action='store_true', help='Query ClickUp data from BigQuery')
    parser.add_argument('--all-contacts', action='store_true', help='Process all contacts from CSV file (all task IDs)')
    args = parser.parse_args()

    try:
        task_data = load_csv_data(args.csv_file)

        if not args.task_ids and not args.all_contacts:
            # Show all available task IDs
            print("Available Task IDs:")
            for task_id in sorted(task_data.keys()):
                # Get the first entry for each task_id for display
                info = task_data[task_id][0] if task_data[task_id] else {}
                name = f"{info.get('first_name', '')} {info.get('last_name', '')}".strip()
                email = info.get('email', '')
                print(f"  {task_id} - {name} ({email})")
            print(f"\nTotal tasks found: {len(task_data)}")
            print("\nUse --all-contacts to process all contacts, or specify task IDs to search for.")
        else:
            if args.all_contacts:
                # Process all task IDs from the CSV file
                all_task_ids = list(task_data.keys())
                print(f"Processing all {len(all_task_ids)} task IDs from CSV file...")
            else:
                # Find specific task IDs
                all_task_ids = args.task_ids

            results = find_task_info(task_data, all_task_ids, use_bigquery=args.bigquery)
            display_results(results, args.output)

    except FileNotFoundError:
        print(f"Error: File '{args.csv_file}' not found")
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()    # Set up argument parser
