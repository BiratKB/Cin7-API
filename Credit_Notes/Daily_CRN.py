import time
import requests
import base64
import datetime
import csv
from dateutil import parser
import pytz
import logging
import os
from concurrent.futures import ThreadPoolExecutor

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
BASE_URL = 'https://api.cin7.com/api/v1/CreditNotes'
FIELDS = 'id,reference,company,firstName,lastName,projectName,source,currencyCode,currencyRate,lineItems,completedDate,invoiceNumber,accountingAttributes'
ROWS_PER_PAGE = 250

ARL_KEY = os.environ["ARL_KEY"]
ARIB_KEY = os.environ["ARIB_KEY"]
ARNL_KEY = os.environ["ARNL_KEY"]
ARF_KEY = os.environ["ARF_KEY"]

# List of user credentials
USERS = [
    {"username": "AlbertRogerUK", "key": ARL_KEY},
    {"username": "AlbertRogerFrancEU", "key": ARF_KEY},
    {"username": "AlbertRogerIberiEU", "key": ARIB_KEY},
    {"username": "AlbertRogerNetheEU", "key": ARNL_KEY}
]

def get_auth_header(username, key):
    credentials = f"{username}:{key}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    return {'Authorization': f'Basic {encoded_credentials}', 'Content-Type': 'application/json'}

def call_api(url, headers):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json(), None
    except requests.RequestException as e:
        return None, str(e)

def parse_date(date_string):
    if not date_string:
        return None

    try:
        parsed_date = parser.parse(date_string)
        if parsed_date.tzinfo is None or parsed_date.tzinfo.utcoffset(parsed_date) is None:
            parsed_date = pytz.utc.localize(parsed_date)
        else:
            parsed_date = parsed_date.astimezone(pytz.utc)
        return parsed_date
    except ValueError as e:
        # Handle specific parsing errors
        logging.warning(f"Failed to parse date: {date_string}. Error: {e}")
        return None
    except Exception as e:
        # Catch any unexpected exceptions and log them
        logging.error(f"Unexpected error parsing date: {date_string}. Error: {e}")
        return None

def calculate_date_range():
    # Set the start and end dates for the year 2024
    start_date = datetime.datetime(2024, 1, 1, tzinfo=pytz.utc)  # (Year, Month, Day, Hour, Minute, Second, ...,)
    end_date = datetime.datetime(2025, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc)  # (Year, Month, Day, Hour, Minute, Second, ...,)

    return start_date, end_date

def is_valid_credit_note(credit_note, start_date, end_date):
    if 'completedDate' not in credit_note:
        logging.warning("Sales order missing 'completedDate'.")
        return False

    invoice_date = parse_date(credit_note['completedDate'])
    if invoice_date is None:
        logging.warning(f"Failed to parse invoice date for sales order {credit_note.get('reference', 'Unknown Reference')}.")
        return False

    return start_date <= invoice_date <= end_date

def process_credit_note(credit_note, user_name):
    line_items = credit_note.get('lineItems', [])
    currency_rate = float(credit_note.get('currencyRate', 1))
    created_date = parse_date(credit_note.get('completedDate'))
    discount_total = credit_note.get('discountTotal', 0)

     # Create a dictionary to map full names to abbreviations
    user_abbreviations = {
        "AlbertRogerUK": "ARL",
        "AlbertRogerNetheEU": "ARNL",
        "AlbertRogerFrancEU": "ARF",
        "AlbertRogerIberiEU": "ARIB"
    }
    
    # Get the abbreviation for the user_name, or use the original if not found
    abbreviated_user_name = user_abbreviations.get(user_name, user_name)
    
    results = []
    num_products = len(line_items)
    
    for item in line_items:
        unit_price = float(item.get('unitPrice', 0))
        discount = float(item.get('discount', 0))
        
        adjusted_unit_price = round(unit_price * currency_rate, 2)
        adjusted_discount = round(discount * currency_rate, 2)
        
        # Distribute discountTotal across all products
        adjusted_discount_total = round((discount_total / num_products) * currency_rate, 2)



        results.append({
            'sourceUser': abbreviated_user_name,
            'accountingAttributes':credit_note.get('accountingAttributes').get('accountingImportStatus'),
            'reference': credit_note.get('reference'),
            'creditNoteNumber':credit_note.get('creditNoteNumber'),
            'salesReference': credit_note.get('salesReference'),
            'createdDate': item.get('createdDate',''),
            'company': credit_note.get('company'),
            'firstName': credit_note.get('firstName'),
            'lastName': credit_note.get('lastName'),
            'projectName': credit_note.get('projectName'),
            'channel': credit_note.get('source'),
            'currencyCode': credit_note.get('currencyCode'),
            'lineItemcode': item.get('code', ''),
            'lineItemName': item.get('name', ''),
            'lineItemQty': item.get('qty', ''),
            'lineItemoption3': item.get('option3',''),
            'lineItemUnitPrice': adjusted_unit_price,
            'lineItemDiscount': adjusted_discount,
            'discountTotal': adjusted_discount_total,
            'completedDate': created_date.strftime('%d/%m/%Y') if created_date else ''

        })
    
    return results

def process_user(user):
    headers = get_auth_header(user['username'], user['key'])
    start_date, end_date = calculate_date_range()
    all_credit_notes = []
    page = 1

    while True:
        url = f'{BASE_URL}?fields={FIELDS}&page={page}&rows={ROWS_PER_PAGE}'
        logging.info(f"Fetching page {page} for user {user['username']}...")

        data, error = call_api(url, headers)
        if error:
            logging.error(f"API call failed for user {user['username']}: {error}")
            break

        if not data:
            logging.info(f"No more data to fetch for user {user['username']}.")
            break

        for credit_note in data:
            try:
                if is_valid_credit_note(credit_note, start_date, end_date):
                    all_credit_notes.extend(process_credit_note(credit_note, user['username']))
            except Exception as e:
                logging.error(f"Error processing sales order: {credit_note}. Error: {e}")
        logging.info(f"Page {page} processed for user {user['username']}.")
        page += 1
        time.sleep(0.5)  # Rate limiting

    return all_credit_notes

def main():
    start_date, end_date = calculate_date_range()
    
    fieldnames = ['sourceUser','accountingAttributes','reference','creditNoteNumber','salesReference','createdDate', 'company', 'firstName', 'lastName', 'projectName', 
                  'channel', 'currencyCode', 'lineItemcode', 'lineItemName','lineItemQty','lineItemoption3', 'lineItemUnitPrice', 'lineItemDiscount', 'discountTotal','completedDate']
    
    file_name = f"Credit_Notes_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
# Saves it in a temporal file 
    output_filename = os.path.join("tmp_files", file_name)
    os.makedirs("tmp_files", exist_ok=True)

    all_credit_notes = []


    # Process users in parallel
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = executor.map(process_user, USERS)
        for user_credit_notes in results:
            all_credit_notes.extend(user_credit_notes)

    # Write all credit notes to a single CSV file
    with open(output_filename, mode='w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for credit_note in all_credit_notes:
            writer.writerow(credit_note)
            
# Export the EXACT path for the workflow
    gh_env = os.getenv('GITHUB_ENV')
    output_filename_abs = os.path.abspath(output_filename) 
    output_filename_base = os.path.basename(output_filename)

    if gh_env:
        with open(gh_env, "a") as env_file:
            env_file.write(f"ENV_CUSTOM_DATE_FILE={output_filename_abs}\n")       
            env_file.write(f"ENV_CUSTOM_DATE_FILE_NAME={output_filename_base}\n")

        logging.info(f"Exported ENV_CUSTOM_DATE_FILE={output_filename_abs}")
        logging.info(f"Exported ENV_CUSTOM_DATE_FILE_NAME={output_filename_base}")

    else:
        logging.warning("GITHUB_ENV not set; cannot export ENV_CUSTOM_DATE_FILE.")

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()