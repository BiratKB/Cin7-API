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
BASE_URL = 'https://api.cin7.com/api/v1/PurchaseOrders'
FIELDS = 'id,reference,company,branchId,internalComments,currencyCode,currencyRate,lineItems,status,stage,projectName,estimatedDeliveryDate,fullyReceivedDate,createdDate,invoiceNumber,isVoid,internalComments'
ROWS_PER_PAGE = 250

ARL_KEY = os.environ["ARL_KEY"]
ARIB_KEY = os.environ["ARIB_KEY"]
ARNL_KEY = os.environ["ARNL_KEY"]
ARF_KEY = os.environ["ARF_KEY"]
# List of user credentials
USERS = [
    {"username":"AlbertRogerUK", "key": ARL_KEY},
    {"username":"AlbertRogerFrancEU","key": ARF_KEY},
    {"username":"AlbertRogerIberiEU", "key": ARIB_KEY},
    {"username":"AlbertRogerNetheEU", "key": ARNL_KEY}
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
    except Exception as e:
        logging.warning(f"Failed to parse date: {date_string}. Error: {e}")
        return None

def calculate_date_range():
    today = datetime.datetime.now(pytz.utc)
    start_date = datetime.datetime(2024, 1, 1, tzinfo=pytz.utc)  # (Year, Month, Day, Hour, Minute, Second, ...,)
    today = today.replace(hour=23, minute=59, second=59, microsecond=999999)
    return start_date, today

def is_valid_purchase_order(purchase_order, start_date, end_date):
    # Check if the purchase order is not void
    is_void = purchase_order.get('isVoid', False)
    if is_void:
        return False

    # Check if the created date is within the last 12 months
    created_date = parse_date(purchase_order.get('createdDate'))
    return created_date and start_date <= created_date <= end_date

def process_purchase_order(purchase_order, user_name):
    line_items = purchase_order.get('lineItems', [])
    currency_rate = float(purchase_order.get('currencyRate', 1))
    estimated_delivery_date = parse_date(purchase_order.get('estimatedDeliveryDate'))
    fully_received_date = parse_date(purchase_order.get('fullyReceivedDate'))
    created_date = parse_date(purchase_order.get('createdDate'))
    results = []
    for item in line_items:
        unit_price = float(item.get('unitPrice', 0))
        discount = float(item.get('discount', 0))
        
        adjusted_unit_price = round(unit_price * currency_rate, 2)
        adjusted_discount = round(discount * currency_rate, 2)

        results.append({
            'sourceUser': user_name,
            'downloadSource': f"Cin7_{user_name}",
            'reference': purchase_order.get('reference'),
            'company': purchase_order.get('company'),
            'branchId':purchase_order.get('branchId'),
            'currencyCode': purchase_order.get('currencyCode'),
            'lineItemcode': item.get('code', ''),
            'lineItemName': item.get('name', ''),
            'status':purchase_order.get('status', ''),
            'stage':purchase_order.get('stage', ''),
            'projectName':purchase_order.get('projectName', ''),
            'internalComments':purchase_order.get('internalComments',''),
            'lineItemQty': item.get('qty', ''),
            'lineItemoption3': item.get('option3', ''),
            'lineItemUnitPrice': adjusted_unit_price,
            'lineItemDiscount': adjusted_discount,
            'createdDate' : created_date.strftime('%d/%m/%Y') if created_date else '',
            'estimatedDeliveryDate': estimated_delivery_date.strftime('%d/%m/%Y') if estimated_delivery_date else '',
            'fullyReceivedDate': fully_received_date.strftime('%d/%m/%Y') if fully_received_date else ''
        })
    
    return results

def process_user(user):
    headers = get_auth_header(user['username'], user['key'])
    start_date, end_date = calculate_date_range()
    all_purchase_orders = []
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

        for purchase_order in data:
            if is_valid_purchase_order(purchase_order, start_date, end_date):
                all_purchase_orders.extend(process_purchase_order(purchase_order, user['username']))

        logging.info(f"Page {page} processed for user {user['username']}.")
        page += 1
        time.sleep(0.5)  # Rate limiting

    return all_purchase_orders

def main():
    
    
    fieldnames = ['downloadSource', 'sourceUser', 'reference', 'company', 'branchId', 'currencyCode', 
    'lineItemcode', 'lineItemName','status','stage','projectName','internalComments', 'lineItemQty', 'lineItemoption3', 'lineItemUnitPrice', 
    'lineItemDiscount', 'createdDate', 'estimatedDeliveryDate', 'fullyReceivedDate']
    
    file_name = f"purchase_orders_Daily.csv"

      # Saves it in a temporal file 
    output_filename = os.path.join("tmp_files", file_name)
    os.makedirs("tmp_files", exist_ok=True)
    all_purchase_orders = []

    # Process users in parallel
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = executor.map(process_user, USERS)
        for user_purchase_orders in results:
            all_purchase_orders.extend(user_purchase_orders)

    # Write all purchase orders to a single CSV file
    with open(output_filename, mode='w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for purchase_order in all_purchase_orders:
            writer.writerow(purchase_order)

    logging.info(f"Data successfully written to {file_name}")
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