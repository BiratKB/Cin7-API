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
BASE_URL = 'https://api.cin7.com/api/v1/SalesOrders'
FIELDS = 'id,reference,customerOrderNo,salesReference,invoiceDate,estimatedDeliveryDate,company,firstName,lastName,projectName,source,currencyCode,currencyRate,lineItems,discountTotal,completedDate,invoiceNumber,taxRate'
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
    start_date = datetime.datetime(2025, 1, 1, tzinfo=pytz.utc)  # (Year, Month, Day, Hour, Minute, Second, ...,)
    end_date = datetime.datetime(2025, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc)  # (Year, Month, Day, Hour, Minute, Second, ...,)

    return start_date, end_date

def is_valid_sales_orders(sales_orders, start_date, end_date):
    if 'invoiceDate' not in sales_orders:
        logging.warning("Sales order missing 'invoiceDate'.")
        return False

    invoice_date = parse_date(sales_orders['invoiceDate'])
    if invoice_date is None:
        logging.warning(f"Failed to parse invoice date for sales order {sales_orders.get('reference', 'Unknown Reference')}.")
        return False

    return start_date <= invoice_date <= end_date


def process_sales_orders(sales_orders, user_name):
    line_items = sales_orders.get('lineItems', [])
    currency_rate = float(sales_orders.get('currencyRate', 1))
    invoice_date = parse_date(sales_orders.get('invoiceDate'))
    estimated_delivery_date = parse_date(sales_orders.get('estimatedDeliveryDate'))
    discount_total = sales_orders.get('discountTotal', 0)

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
            'reference': sales_orders.get('reference'),
            'invoiceNumber':sales_orders.get('invoiceNumber'),
            'customerOrderNo':sales_orders.get('customerOrderNo'),
            'estimatedDeliveryDate': estimated_delivery_date.strftime('%d/%m/%Y') if invoice_date else '',
            'company': sales_orders.get('company'),
            'firstName': sales_orders.get('firstName'),
            'lastName': sales_orders.get('lastName'),
            'projectName': sales_orders.get('projectName'),
            'channel': sales_orders.get('source'),
            'currencyCode': sales_orders.get('currencyCode'),
            'lineItemcode':item.get('code',''),
            'lineItemName': item.get('name', ''),
            'lineItemQty': item.get('qty', ''),
            'lineItemoption3': item.get('option3',''),
            'lineItemUnitPrice': adjusted_unit_price,
            'lineItemDiscount': adjusted_discount,
            'discountTotal': adjusted_discount_total,            
            'invoiceDate': invoice_date.strftime('%d/%m/%Y') if invoice_date else ''

        })
    
    return results

def process_user(user):
    headers = get_auth_header(user['username'], user['key'])
    start_date, end_date = calculate_date_range()
    all_sales_orders = []
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

        for sales_orders in data:
            try:
                if is_valid_sales_orders(sales_orders, start_date, end_date):
                    all_sales_orders.extend(process_sales_orders(sales_orders, user['username']))
            except Exception as e:
                logging.error(f"Error processing sales order: {sales_orders}. Error: {e}")

        logging.info(f"Page {page} processed for user {user['username']}.")
        page += 1
        time.sleep(0.5)  # Rate limiting

    return all_sales_orders

def main():
    start_date, end_date = calculate_date_range()
    
    fieldnames = ['sourceUser','reference', 'invoiceNumber','customerOrderNo','estimatedDeliveryDate','company', 'firstName', 'lastName', 'projectName',
                  'channel', 'currencyCode','lineItemcode', 'lineItemName','lineItemQty','lineItemoption3', 'lineItemUnitPrice', 'lineItemDiscount', 'discountTotal','invoiceDate']
    
    file_name = f"Sales_Orders_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"

     # Saves it in a temporal file 
    output_filename = os.path.join("tmp_files", file_name)
    os.makedirs("tmp_files", exist_ok=True)

    all_sales_orders = []

    # Process users in parallel
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = executor.map(process_user, USERS)
        for user_sales_orders in results:
            all_sales_orders.extend(user_sales_orders)

    # Write all sales orders to a single CSV file
    with open(output_filename, mode='w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for sales_orders in all_sales_orders:
            writer.writerow(sales_orders)

    logging.info(f"Data successfully written locally at {output_filename}")

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