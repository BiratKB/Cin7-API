import time
import requests
import base64
import datetime
import pandas as pd
from dateutil import parser
import pytz
import logging
import os
from concurrent.futures import ThreadPoolExecutor

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
BASE_URL = 'https://api.cin7.com/api/v1/SalesOrders'
FIELDS = 'id,reference,customerOrderNo,salesReference,invoiceDate,createdDate,company,firstName,lastName,branchId,projectName,source,currencyCode,currencyRate,lineItems,discountTotal,completedDate,invoiceNumber,customFields'
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

def classify_entity(row):
    company = str(row["company"]).upper()
    source_user = str(row["sourceUser"])  # Use sourceUser here
    branch_id = str(row["branchId"]).upper()
    
    # Ensure item_code is retrieved correctly
    item_code = str(row.get("Item Code", "")).upper()
    
    # User Abbreviations
    user_abbreviations = {
        "AlbertRogerUK": "ARL",
        "AlbertRogerNetheEU": "ARNL",
        "AlbertRogerFrancEU": "ARF",
        "AlbertRogerIberiEU": "ARIB"
    }
    
    # Get abbreviated username or original if not found
    abbreviated_user = user_abbreviations.get(source_user, source_user)

    user_and_branch = f"{abbreviated_user}{branch_id}"  # Combined sourceUser and branch ID

    # Classification based on company name
    if "ALBERT ROGER" in company and company != "ALBERT ROGER IBERICA":
        return "XWh"
    elif "TESTER" in company:
        return "XWh"
    elif "CARREFOUR" in company:
        return "XWH"

    if f"{abbreviated_user}{branch_id}{item_code[:4]}" == "ARN398RECF":
        return source_user + "-LGI"

    # Check line items if present
    line_items = row.get('lineItems', [])

    for line_item in line_items:
        item_code = str(line_item.get('lineItemcode', '')).upper()
        if user_and_branch == "ARL726" and item_code.startswith("NBNA"):
            return source_user+ "-P&P"


    # Classification based on combined user and branch
    if user_and_branch in ["ARL726", "ARL3", "ARL916", "ARL977", "ARL1007"]:
        return source_user + "-P&P"
    elif user_and_branch in ["ARL777", "ARL4", "ARL5", "ARL863", "ARL47", "ARL779", "ARL856", "ARL875",
                             "ARL1019", "ARL937", "ARL936", "ARIB3", "ARF179", "ARF3", "ARF378",
                             "ARF262", "ARF402", "ARF454"]:
        return source_user +"-BCN"
   
    elif user_and_branch in ["ARL969"]:
        return source_user +"-PCC"
   
    elif user_and_branch in ["ARL970", "ARL997"]:
        return source_user +"-DMW"
   
    elif user_and_branch == "ARL997":
        return source_user +"-DMW Promo"
   
    elif user_and_branch in ["ARF180", "ARNL130", "ARNL132", "ARNL3", "ARNL336"]:
        return source_user +"-NCP"
    elif user_and_branch == "ARF184":
        return source_user +"-BLN"
   
    elif user_and_branch == "ARF182":
        return source_user +"-LGI"
   
    elif user_and_branch == "ARF277":
        return "XWH"

    # Default case
    return None


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
        return None
    except Exception as e:
  
        return None


def calculate_date_range():
    # Set the start and end dates for the year 2024
    start_date = datetime.datetime(2024, 1, 1, tzinfo=pytz.utc)  
    end_date = datetime.datetime(2025, 12, 31, 23, 59, 59, 999999, tzinfo=pytz.utc)  

    return start_date, end_date

def is_valid_sales_orders(sales_orders, start_date, end_date):
    if 'invoiceDate' not in sales_orders:
        return False

    invoice_date = parse_date(sales_orders['invoiceDate'])
    if invoice_date is None:
  
        return False

    return start_date <= invoice_date <= end_date



def process_sales_orders(sales_orders, user_name):
    line_items = sales_orders.get('lineItems', [])
    currency_rate = float(sales_orders.get('currencyRate', 1))
    invoice_date = parse_date(sales_orders.get('invoiceDate'))
    created_date = parse_date(sales_orders.get('createdDate'))
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
        adjusted_discount_total = round((discount_total / num_products) * currency_rate, 2)

        results.append({
            'sourceUser': abbreviated_user_name,
            'reference': sales_orders.get('reference'),
            'company': sales_orders.get('company'),
            'firstName': sales_orders.get('firstName'),
            'lastName': sales_orders.get('lastName'),
            'createdDate': created_date.strftime('%d/%m/%Y') if created_date else '',
            'branchId': sales_orders.get('branchId',''),
            'currencyCode': sales_orders.get('currencyCode'),
            'lineItemcode': item.get('code', ''),
            'lineItemQty': item.get('qty', ''),
            'lineItemUnitPrice': adjusted_unit_price,
            'lineItemoption3': item.get('option3', ''),
            'customFieldsorders_1001': sales_orders.get('customFields').get('orders_1001'),
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
            # Continue to the next page or break based on your requirements
            break

        if not data:
            logging.info(f"No more data to fetch for user {user['username']}.")
            break

        for sales_orders in data:
            try:
                if is_valid_sales_orders(sales_orders, start_date, end_date):
                    all_sales_orders.extend(process_sales_orders(sales_orders, user['username']))
            except Exception as e:
                logging.error(f"Error processing sales order {sales_orders.get('reference', 'Unknown Reference')}: {sales_orders}. Error: {e}")

        logging.info(f"Page {page} processed for user {user['username']}.")
        page += 1
        time.sleep(0.5)  # Rate limiting

    return all_sales_orders

def main():
    start_date, end_date = calculate_date_range()
    
    fieldnames = ['sourceUser', 'reference', 'company', 'firstName', 'lastName', 'createdDate', 'branchId',
                  'currencyCode', 'lineItemcode', 'lineItemQty', 'lineItemUnitPrice', 'lineItemoption3',
                  'customFieldsorders_1001', 'lineItemDiscount', 'discountTotal', 'invoiceDate']
    
    os.makedirs("tmp_files", exist_ok=True)
    file_name = f"tmp_files/Sales_Orders_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.xlsx"

    
    all_sales_orders = []

    # Process users in parallel
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = executor.map(process_user, USERS)
        for user_sales_orderss in results:
            all_sales_orders.extend(user_sales_orderss)

    # Create DataFrame
    df = pd.DataFrame(all_sales_orders, columns=fieldnames)
    
    # Apply classification function
    df["Warehouse"] = df.apply(classify_entity, axis=1)
    
    # Save to Excel
    df.to_excel(file_name, index=False, engine='openpyxl')
    
    env_file = os.getenv('GITHUB_ENV')
    with open(env_file, "a") as env_file:
        env_file.write(f"ENV_CUSTOM_DATE_FILE_NAME={file_name}")
    
    logging.info(f"Data successfully written to {file_name}")
    logging.info(f"Date range used for filtering: Start: {start_date.strftime('%Y-%m-%d %H:%M:%S %Z')} - End: {end_date.strftime('%Y-%m-%d %H:%M:%S %Z')}")

if __name__ == "__main__":
    main()
    