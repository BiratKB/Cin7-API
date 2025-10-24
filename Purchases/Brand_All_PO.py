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
#All base requirements

logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
#Set up logging

#Config 
BASE_URL = 'https://api.cin7.com/api/v1/PurchaseOrders'
FIELDS = 'id,reference,company,branchId,internalComments,currencyCode,currenyRate,lineItems,status,' \
'stage,projectName,estimatedDeliveryDate,fullyReceivedDate,createdDate,invoiceNumber,isVoid,internalComments'
ROWS_PER_PAGE = 250

#Get API access keys
ARL_KEY = os.environ["ARL_KEY"]
ARNL_KEY = os.environ["ARNL_KEY"]
ARF_KEY = os.environ["ARF_KEY"]
ARIB_KEY = os.environ["ARIB_KEY"]

#Set user access
USERS = [
    {"username":"AlbertRogerUK", "key": ARL_KEY},
    {"username":"AlbertRogerNetheEU", "key": ARNL_KEY},
    {"username":"AlbertRogerFrancEU", "key": ARF_KEY},
    {"username":"AlbertRogerIberiEU", "key": ARIB_KEY}
]

#Company - Brand dictionary
Brand = {
    "CARBON THEORY LTD": "Carbon Theory",
    "COHAR LTD": "Sosu"
}

def get_auth_header(username, key):
    credentials = f"{username}:{key}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    return {'Authorization': f'Basic {encoded_credentials}', 'Content-Type': 'application/json'}

#Call API
def call_api(url, headers):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json(), None
    except requests.RequestException as e:
        return None, str(e)

#Parse date
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

#Define the date range
def calculate_date_range():
    today = datetime.datetime.now(pytz.utc)
    start_date = datetime.datetime(2024, 1, 1, tzinfo=pytz.utc) ##YYYY, MM, DD, hhh, mm, ss
    since_sunday = (today.weekday() - 6)%7 #Monday-0 ,,,, Sunday-6
    last_sunday = today - datetime.timedelta(days=since_sunday)
    last_sunday = last_sunday.replace(hour=23, minute=59, second=59, microsecond=999999)
    return start_date, last_sunday

#Valid PO
def valid_purchase_order(purchase_order, start_date, end_date):
    invoice_date = parse_date(purchase_order.get('fullyReceivedDate'))
    #Check for Void
    is_void = purchase_order.get('isVoid', False)
    if is_void:
        return False
    
    #Check if requested brand
    req_brand = brand[purchase_order.get('company')]

    #Check for valid date range

    return invoice_date and start_date <= invoice_date <=end_date

def main():
    pass





if __name__ == "__main__":
    main()