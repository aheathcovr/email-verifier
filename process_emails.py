import csv
import concurrent.futures
import requests
import json
import logging
import os
import time

# Configuration
INPUT_FILE = "View Users 251216 - Filtered.csv"
OUTPUT_FILE = "verified_emails_output.csv"
API_URL = "http://localhost:8080/api/validate"
MAX_WORKERS = 20
BATCH_TIMEOUT = 10  # Seconds

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def verify_email(email):
    """
    Verifies a single email against the local API.
    Returns a dictionary of results.
    """
    try:
        response = requests.get(API_URL, params={'email': email}, timeout=BATCH_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        # Extract fields
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

def process_emails():
    # Check if input file exists
    if not os.path.exists(INPUT_FILE):
        logger.error(f"Input file {INPUT_FILE} not found.")
        return

    # Read all rows
    rows = []
    input_fieldnames = []
    email_col = None

    try:
        with open(INPUT_FILE, mode='r', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            input_fieldnames = reader.fieldnames
            
            # Find the email column (case-insensitive search)
            for field in input_fieldnames:
                if field.lower() == 'email':
                    email_col = field
                    break
            
            if not email_col:
                 logger.error(f"Column 'email' not found in CSV. Available columns: {input_fieldnames}")
                 return
            
            for row in reader:
                rows.append(row)
                
    except Exception as e:
        logger.error(f"Failed to read CSV: {e}")
        return
    
    logger.info(f"Loaded {len(rows)} rows to process.")

    # Define validation columns
    # I renamed status -> validation_status and score -> validation_score to avoid potential conflicts
    # error -> validation_error
    validation_fieldnames = [
        'validation_status', 'validation_score', 'syntax', 'domain_exists', 
        'mx_records', 'is_disposable', 'is_role_based', 
        'alias_of', 'typo_suggestion', 'validation_error'
    ]
    
    # Combine headers: Original + Validation
    output_fieldnames = input_fieldnames + [f for f in validation_fieldnames if f not in input_fieldnames]

    # Process concurrently
    processed_count = 0
    
    # Initialize output file (overwrite if exists)
    with open(OUTPUT_FILE, mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=output_fieldnames)
        writer.writeheader()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Create a list of futures. Note: We only process rows that have an email.
            # If a row has empty email, we skip verification but still write it?
            # User probably wants all rows.
            
            future_to_row = {}
            for row in rows:
                email = row.get(email_col)
                if email:
                    future = executor.submit(verify_email, email)
                    future_to_row[future] = row
                else:
                    # No email? Just write the row as is
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
                    # Write row anyway with error?
                    row['validation_error'] = str(exc)
                    writer.writerow(row)
                    
    logger.info("Processing complete.")

if __name__ == "__main__":
    process_emails()
