import csv
import os

def filter_csv():
    input_file = 'verified_emails_output.csv'
    output_file = 'cleaned view user list.csv'

    # List of org_codes to filter out
    excluded_org_codes = {
        "CCS","GCLTC","RHG","SLTC","VOLH","MSTG","VOLH / MSTG","CSC","AVIANA", "SLTC", "PCHC", "BONA", "CORHP", "CSNHC", "DHM", "HILLHG",
        "NEX", "OVHC", "EVEREST", "AVALA", "ELVT", "ECARE", "ELEVHC", "KEREM",
        "MHCS", "ASCEND", "FCC2", "SSS", "TBHS", "WESH", "LOH", "AVAN", "ENG"
    }

    # Org codes for View Labor types
    view_labor_org_codes = {
        "ASPIRE", "CARA", "CSJI", "FUNDAMENTAL", "HMGS", "JACO", "PMHCC", "PMHCCA", "RUBY"
    }

    # Org codes for QRM Labor types
    qrm_labor_org_codes = {
        "ASHFORD", "BLR", "BRIGHTS", "CAS", "CCYOU", "DHC", "EHMG", "FPACP", "GEMINI", "TGR", "VOLH"
    }

    # Org codes for Churned View Corp
    churned_view_corp_org_codes = {
        "CONQ", "EXCEL", "ABBEY", "BEHC", "ARI", "AristaRecovery", "NHG", "PEAC", "PRIME", "KEY", "WEC", "CMG", "RHHR", "DIA", "CAP", "PGC", "EAC", "ANEW", "HCG", "AVHS", "SMTHC", "CWR", "AWAR", "SSH", "BRISTOL", "IRCC", "MWDNC", "SSN", "CHPK", "BHCG"
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

            # Add the new column to fieldnames
            fieldnames = list(reader.fieldnames) + ['View User type']

            with open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()

                count_kept = 0
                count_removed = 0

                for row in reader:
                    # Strip whitespace from org_code for accurate matching
                    org_code = row['org_code'].strip() if row['org_code'] else ""

                    if org_code in excluded_org_codes:
                        count_removed += 1
                    else:
                        # Determine View User type
                        facilities = row.get('facilities', '').strip()

                        if org_code in view_labor_org_codes:
                            # Check if facilities is blank or has multiple facilities
                            if not facilities or ',' in facilities:
                                view_user_type = "View Labor - Corp"
                            else:
                                view_user_type = "View Labor - facility"
                        elif org_code in qrm_labor_org_codes:
                            # Check if facilities is blank or has multiple facilities
                            if not facilities or ',' in facilities:
                                view_user_type = "QRM Labor - Corp"
                            else:
                                view_user_type = "QRM Labor - facility"
                        elif org_code in churned_view_corp_org_codes:
                            # Churned View Corp for specific org_codes
                            view_user_type = "Churned View Corp"
                        else:
                            # Default for all other org_codes
                            view_user_type = "View - No Labor"

                        # Add the new column to the row
                        row['View User type'] = view_user_type

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
