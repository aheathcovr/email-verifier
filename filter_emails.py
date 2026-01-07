import csv
import os

def filter_csv():
    input_file = 'verified_emails_output.csv'
    output_file = 'cleaned view user list.csv'

    # List of org_codes to filter out
    excluded_org_codes = {
        "AVIANA", "SLTC", "PCHC", "BONA", "CORHP", "CSNHC", "DHM", "HILLHG", 
        "NEX", "OVHC", "EVEREST", "AVALA", "ELVT", "ECARE", "ELEVHC", "KEREM", 
        "MHCS", "ASCEND", "FCC2", "SSS", "TBHS", "WESH", "LOH", "AVAN", "ENG"
    }

    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found in the current directory.")
        return

    try:
        with open(input_file, mode='r', newline='', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            
            # Check if expected column exists
            if 'org_code' not in reader.fieldnames:
                print(f"Error: 'org_code' column not found in {input_file}")
                print(f"Found columns: {', '.join(reader.fieldnames)}")
                return

            with open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
                writer.writeheader()

                count_kept = 0
                count_removed = 0

                for row in reader:
                    # Strip whitespace from org_code for accurate matching
                    org_code = row['org_code'].strip() if row['org_code'] else ""
                    
                    if org_code in excluded_org_codes:
                        count_removed += 1
                    else:
                        writer.writerow(row)
                        count_kept += 1
        
        print(f"Filtering complete.")
        print(f"Rows kept: {count_kept}")
        print(f"Rows removed: {count_removed}")
        print(f"Output saved to: {output_file}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    filter_csv()
