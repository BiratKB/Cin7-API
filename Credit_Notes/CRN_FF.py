#Set up libraries
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

#Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#API config setup
BASE_URL = 'https://api.cin7.com/api/v1/CreditNotes'
FIELDS = 'id, reference, creditNoteNumber, salesReference, createdDate, company, firstName, lastName,projectName, source, currencyCode, currencyRate, lineItems, discountTotal, completedDate, invoiceNumber'
ROWS_PER_PAGE = 250

#Get API Key
ARL_KEY = os.environ["ARL_KEY"]
ARNL_KEY = os.environ["ARNL_KEY"]
ARF_KEY = os.environ["ARF_KEY"]
ARIB_KEY = os.environ["ARIB_KEY"]

#Set user credentials
USERS = [
    {"username":"AlbertRogerUK", "key": ARL_KEY},
    {"username":"AlbertRogerNetheEU", "key": ARNL_KEY},
    {"username":"AlbertRogerFrancEU", "key": ARF_KEY},
    {"username":"AlbertRogerIberiEU", "key": ARIB_KEY}
]

#Get https authorization code
def get_auth_header(username, key):
    credentials = f"{username}:{key}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    return {'Authorization': f'Basic {encoded_credentials}', 'Content-Type': 'application/json'}

#Def call API
def call_api(url, headers):
    try:
        response = requests.get(url, headers = headers)
        response.raise_for_status()
        return response.json(), None
    except requests.RequestException as e:
        return None, str(e)
    
#Def Parse Date
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
        #Handle specific parsing errors
        logging.warning(f"Failed to parse date: {date_string}. Error: {e}")
        return None
    except Exception as e:
        #Catch other unexpected errors
        logging.error(f"Unexpected error parsing data: {date_string}. Error: {e}")
        return None
    
#Set date range
def calculate_date_range():
    today = datetime.datetime.now(pytz.utc)
    start_dt = today - datetime.timedelta(days=7)
    start_dt = start_dt.replace(hour=14, minute=0, second=0, microsecond=0)
    end_dt = today
    end_dt = end_dt.replace(hour=13, minute=59, microsecond=999999)
    return start_dt, end_dt

#Check if CRN is valid
def is_valid_credit_note(credit_note, start_date, end_date):
    if 'completedDate' not in credit_note:
        logging.warning("Sales order missing 'completedDate'.")
        return False
    
    invoice_date = parse_date(credit_note['completedDate'])
    if invoice_date is None:
        logging.warning(f"Failed to parse invoice date for sales order {credit_note.get('reference', 'Unknown Reference')}.")
        return False
    
    return start_date <= invoice_date <= end_date

#Main process credit note
def process_credit_note(credit_note, user_name):
    line_items = credit_note.get('lineItems', [])
    currency_rate = float(credit_note.get('currencyRate', 1))
    created_date = parse_date(credit_note.get('completedDate'))
    discount_total = credit_note.get('discountTotal', 0)

    #Map full names to short
    user_abbreviations = {
        "AlbertRogerUK": "ARL",
        "AlbertRogerNetheEU": "ARNL",
        "AlbertRogerFrancEU": "ARF",
        "AlbertRogerIberiEU": "ARIB"
    }

    #Use abbreviation when possible
    abbreviated_user_name = user_abbreviations.get(user_name, user_name)

    results = []
    num_products = len(line_items)

    for item in line_items:
        unit_price = float(item.get('unitPrice', 0))
        discount = float(item.get('discount', 0))

        adjusted_unit_price = round(unit_price * currency_rate, 2)
        adjusted_discount = round(discount * currency_rate, 2)

        #Distribute discountTotal across all products
        adjusted_discount_total = round((discount_total / num_products) * currency_rate, 2)

        results.append({
            'sourceUser': abbreviated_user_name,
            'reference': credit_note.get('reference'),
            'creditNoteNumber': credit_note.get('creditNoteNumber'),
            'salesReference': credit_note.get('salesReference'),
            'createdDate': item.get('createdDate', ''),
            'company': credit_note.get('company'),
            'firstName': credit_note.get('firstName'),
            'lastName': credit_note.get('lastName'),
            'projectName': credit_note.get('projectName'),
            'channel': credit_note.get('source'),
            'currencyCode':credit_note.get('currencyCode'),
            'lineItemcode': item.get('code', ''),
            'lineItemName': item.get('name', ''),
            'lineItemQty': item.get('qty', ''),
            'lineItemoption3': item.get('option3', ''),
            'lineItemUnitPrice': adjusted_unit_price,
            'lineItemDiscount': -adjusted_discount,
            'discountTotal': adjusted_discount_total,
            'completedDate': created_date.strftime('%d/%m/%Y') if created_date else ''
        })
    return results


#Def process user (entities)
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
        time.sleep(0.5) #Limit rate

    return all_credit_notes

#Set up main
def main():
    start_date, end_date = calculate_date_range()
    fieldnames = ['sourceUser','reference','creditNoteNumber','salesReference','createdDate','company',
                  'firstName','lastName','projectName','channel','currencyCode','lineItemcode','lineItemName',
                  'lineItemQty','lineItemoption3','lineItemUnitPrice','lineItemDiscount','discountTotal','completedDate']
    
    file_name = f"Credit_Notes_FF_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%y%m%d')}.csv"

    #Save in temporal file
    output_filename = os.path.join("tmp_files", file_name)
    os.makedirs("tmp_files", exist_ok=True)

    all_credit_notes = []

    #Process users in parallel
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = executor.map(process_user, USERS)
        for user_credit_notes in results:
            all_credit_notes.extend(user_credit_notes)

    #All in 1 csv
    with open(output_filename, mode='w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for credit_note in all_credit_notes:
            writer.writerow(credit_note)
    
    logging.info(f"Data successfully written locally at {output_filename}")

#Export the exact path for the workflow
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
        logging.warning("GITHUB_ENV not set; cannot export ENV CUSTOM_DATE_FILE.")

if __name__ == "__main__":
    main()