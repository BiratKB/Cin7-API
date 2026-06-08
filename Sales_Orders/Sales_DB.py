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

#Set up logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#API config
BASE_URL = 'https://api.cin7.com/api/v1/SalesOrders'
FIELDS = 'reference, company, firstName, lastName, createdDate, branchId, projectName, currencyCode, ' \
'lineItems, directOrder, isVoid'
ROWS_PER_PAGE = 250

#Set user credentials
ARL_KEY = os.environ["ARL_KEY"]
ARNL_KEY = os.environ["ARNL_KEY"]
ARF_KEY = os.environ["ARF_KEY"]
ARIB_KEY = os.environ["ARIB_KEY"]

USERS = [
    {"username":"AlbertRogerUK", "key": ARL_KEY},
    {"username":"AlbertRogerNetheEU", "key": ARNL_KEY},
    {"username":"AlbertRogerFrancEU", "key": ARF_KEY},
    {"username":"AlbertRogerIberiEU", "key": ARIB_KEY}
]


def get_auth_header(username, key):
    credentials = f"{username}:{key}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    return {'Authorization': f'Basic {encoded_credentials}', 'Content-Type': 'application/json'}

#API call function with retry for error

def call_api(url, headers, max_retries=5, base_delay=1):
    """
    Calls the given API URL with retries and exponential backoff.
    Retries on 429 (Too Many Requests) or 5xx errors.

    Args:
        url (str): API endpoint to call.
        headers (dict): Headers to include in the request.
        max_retries (int): Maximum retry attempts before failing.
        base_delay (int): Base delay in seconds for exponential backoff.

    Returns:
        tuple: (json_data, error_message)
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers)

            # Handle rate limit (429) explicitly
            if response.status_code == 429:
                wait_time = base_delay * (2 ** attempt)  # Exponential backoff
                logging.warning(
                    f"Rate limit hit (429) on attempt {attempt + 1}/{max_retries}. "
                    f"Retrying in {wait_time}s..."
                )

                # Optionally use 'Retry-After' header if Cin7 provides it
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    wait_time = int(retry_after)

                time.sleep(wait_time)
                continue  # retry the request

            # Retry on server errors (5xx)
            if 500 <= response.status_code < 600:
                wait_time = base_delay * (2 ** attempt)
                logging.warning(
                    f"Server error {response.status_code} on attempt {attempt + 1}/{max_retries}. "
                    f"Retrying in {wait_time}s..."
                )
                time.sleep(wait_time)
                continue

            # Success: no errors
            response.raise_for_status()
            return response.json(), None

        except requests.RequestException as e:
            wait_time = base_delay * (2 ** attempt)
            logging.warning(
                f"Request error on attempt {attempt + 1}/{max_retries}: {e}. "
                f"Retrying in {wait_time}s..."
            )
            time.sleep(wait_time)

    # If all retries failed
    error_message = f"Failed after {max_retries} attempts: {url}"
    logging.error(error_message)
    return None, error_message
    
#Parse date function
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
        #Catch date parse errors
        logging.warning(f"Failed to parse data: {date_string}. Error: {e}")
        return None
    except Exception as e:
        #Catch and log other exceptions
        logging.error(f"Unexpected error parsing date: {date_string}. Error:{e}")
        return None
    
#Calculate date-time range
def calculate_date_range():
    today = datetime.datetime.now(pytz.utc)
    this_monday = today - datetime.timedelta(days=today.weekday())
    this_monday = this_monday.replace(hour=0, minute=0, second=0, microsecond=0)
    start_dt = (this_monday - datetime.timedelta(weeks=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_dt = (this_monday - datetime.timedelta(seconds=1)).replace(hour=23, minute=59, second=59, microsecond=999999)
    return start_dt, end_dt


#Check valid orders : no Void and created within last week 
def is_valid_sales_orders(sales_orders, start_date, end_date):
    is_void = sales_orders.get('isVoid', False)
    if is_void:
        return False
    
    if 'createdDate' not in sales_orders:
        logging.warning("Sales order missing 'createdDate'.")
        return False
    
    created_date = parse_date(sales_orders['createdDate'])
    if created_date is None:
        logging.warning(f"Failed to parse created date for sales order {sales_orders.get('reference', 'Unknown Reference')}.")
        return False
    
    return start_date <= created_date <= end_date

def process_sales_orders(sales_orders, user_name):
    line_items = sales_orders.get('lineItems', [])
    created_date = parse_date(sales_orders.get('createdDate'))

    results = []
    num_products = len(line_items)

    for item in line_items:
        results.append({
            'Order Ref': sales_orders.get('reference'),
            'Company': sales_orders.get('company'),
            'First Name': sales_orders.get('firstName'),
            'Last Name': sales_orders.get('lastName'),
            'Created Date': created_date.strftime('%d/%m/%Y') if created_date else '',
            'Branch ID': sales_orders.get('branchId'),
            'Project Name': sales_orders.get('projectName'),
            'Currency Name': sales_orders.get('currencyCode'),
            'Item Code': item.get('code',''),
            'Item Qty': item.get('qty',''),
            'Item Price': item.get('unitPrice',''),
            'Item Option 3': item.get('option3',''),
            'Direct Order': sales_orders.get('directOrder')
        })

    return results

def process_user(user):
    headers = get_auth_header(user['username'], user['key'])
    start_date, end_date = calculate_date_range()
    all_sales_orders = []
    page = 1

    while True:
        url = (f"{BASE_URL}"f"?fields={FIELDS}"f"&page={page}"f"&rows={ROWS_PER_PAGE}"f"&where = createdDate >= '{start_date}' AND createdDate <= '{end_date}'")
        logging.info(f"Fetching page {page} for user {user['username']}...")

        data, error = call_api(url, headers)
        if error:
            logging.error(f"API call failed for user {user['username']}: {error}")
            break

        if not data:
            logging.error(f"No more data to fetch for user {user['username']}.")
            break

        for sales_orders in data:
            try:
                if is_valid_sales_orders(sales_orders, start_date, end_date):
                    all_sales_orders.extend(process_sales_orders(sales_orders, user['username']))
            except Exception as e:
                logging.error(f"Error processing sales order: {sales_orders}. Error: {e}")
        
        logging.info(f"Page {page} processed for user {user['username']}.")
        page += 1
        time.sleep(0.5)

    return all_sales_orders

def main():
    #Calculate last week range :)
    start_date, end_date = calculate_date_range()

    fieldnames = [
        'Order Ref',
        'Company',
        'First Name',
        'Last Name',
        'Created Date',
        'Branch ID',
        'Project Name',
        'Currency Name',
        'Item Code',
        'Item Qty',
        'Item Price',
        'Item Option 3',
        'Direct Order'
    ]

    #Make temp output dir.
    os.makedirs("tmp_files", exist_ok=True)

    #Process users in parallel
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = executor.map(process_user, USERS)

        #Write one CSV per user
        for user, user_sales_orders in zip(USERS, results):
            username = user["username"]

            #Name CSV with date range
            file_name = (
                f"Sales__Orders_{username}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
            )
            output_filename = os.path.join("tmp_files", file_name)

            #Sort by created date
            try:
                user_sales_orders.sort(
                    key = lambda x: datetime.datetime.strptime(x['Created Date'], "%d/%m/%Y")
                    if x['Created Date'] else datetime.datetime.min
                )
            except Exception as e:
                logging.warning(f"Could not sort data for {username}: {e}")

            #Write user's data to their csv
            with open(output_filename, mode='w', newline='', encoding='utf-8') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writeheader()
                for sales_order in user_sales_orders:
                    writer.writerow(sales_order)

            logging.info(f"Data successfuly written for {username} at {output_filename}")

            #Export for workflow
            gh_env = os.getenv('GITHUB_ENV')
            output_filename_abs = os.path.abspath(output_filename)
            output_filename_base = os.path.basename(output_filename)

            if gh_env:
                with open(gh_env, "a") as env_file:
                    env_file.write(f"ENV_CUSTOM_DATE_FILE_{username.upper()}={output_filename_abs}\n")
                    env_file.write(f"ENV_CUSTOM_DATE_FILE_NAME_{username.upper()}={output_filename_base}\n")

                logging.info(f"Exported ENV_CUSTOM_DATE_FILE_NAME_{username.upper()}={output_filename_abs}")
            else:
                logging.warning("GITHUB_ENV not set; cannot export ENV_CUSTOME_DATE_FILE.")

if __name__ == "__main__":
    main()