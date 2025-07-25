from datetime import datetime, timedelta, timezone
import time
import os
import argparse
from dotenv import load_dotenv
import boto3
import json
import requests
import logging
import tqdm
import xml.etree.ElementTree as ET

parser = argparse.ArgumentParser()
parser.add_argument('--days', type=float, default=1.0,
                    help='Number of days to get updated data')


# ---------- Logger Setup ----------
logger = logging.getLogger()
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


load_dotenv()

AUTHORIZATION_KEY = os.environ.get("AUTHORIZATION_KEY")
LARVOL_KEY = os.environ.get("LARVOL_KEY")
ONCO_PARTITION_KEY = 'source_id'
PRODUCT_PARTITION_KEY = 'li_id'

# DynamoDB setup (uses Lambda's IAM role)
# aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
# aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
# aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

dynamodb = boto3.resource(
    'dynamodb',
    # region_name=aws_region,
    # aws_access_key_id=aws_access_key,
    # aws_secret_access_key=aws_secret_key
)
onco_table = dynamodb.Table('onco_trial_data')
product_table = dynamodb.Table('product_moa')


# Functions to update Trial Data
def get_yesterday_timestamp(days=1):
    "To get previous day's timestamp since when the last job was run"

    now = datetime.now(timezone.utc)
    previous_day = now - timedelta(days)
    return int(previous_day.timestamp())

def get_page_ids(timestamp, page):
    "Get all the Trial IDs on page as passed in input"
    
    try:
        url = f"https://lt.larvol.com/OncotrialsApi.php?timestamp={timestamp}&page={page}"
        headers = {"Authorization": LARVOL_KEY}
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            result = res.json()['source_ids'].split(',')
            result[0] = result[0].replace('[', '')
            result[-1] = result[-1].replace(']', '')
            return result
    except Exception as e:
        logger.error(f"Error fetching page {page}: {e}")
    return []

def get_updated_ids(timestamp):
    "Get all the updated IDs since the last timestamp"
    
    try:
        url = f"https://lt.larvol.com/OncotrialsApi.php?timestamp={timestamp}"
        headers = {"Authorization": LARVOL_KEY}
        res = requests.get(url, headers=headers)

        if res.status_code == 200:
            json_response = res.json()
            total_pages = json_response.get("total_pages")
            print(f"Total Pages: {total_pages}")
            updated_ids = []
            for page in tqdm.tqdm(range(1, total_pages + 1)):
                updated_ids.extend(get_page_ids(timestamp, page))
            return list(set(updated_ids))
    except Exception as e:
        logger.error(f"Error fetching updated IDs: {e}")
    return []

def get_trial_data(id):
    url = f"https://lt.larvol.com/OncotrialsApi.php?source_id={id}"
    headers = {"Authorization": LARVOL_KEY}
    try:
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            return res.json()
        else:
            logger.warning(f"No data for ID {id}")
    except Exception as e:
        logger.error(f"Error fetching data for ID {id}: {e}")
    return None



# Functions to get product MOA map
def get_product_data(id):
    url = f"http://api.larvolinsight.com/api.ashx?tablename=product&id={id}"
    headers = {
            "Authorization" : AUTHORIZATION_KEY,
            "x-api-key": LARVOL_KEY
        }
    res = requests.post(url, headers=headers)
    return res.text

def xml_to_dict(element):
    """Recursively convert an XML element and its children to a dictionary."""
    node = {}
    
    # Add element attributes if any
    if element.attrib:
        node.update({f"@{k}": v for k, v in element.attrib.items()})

    # Add child elements
    children = list(element)
    if children:
        child_dict = {}
        for child in children:
            child_result = xml_to_dict(child)
            if child.tag in child_dict:
                if not isinstance(child_dict[child.tag], list):
                    child_dict[child.tag] = [child_dict[child.tag]]
                child_dict[child.tag].append(child_result)
            else:
                child_dict[child.tag] = child_result
        node.update(child_dict)
    else:
        # Add text content
        node = element.text.strip() if element.text else None

    return node

def parse_xml_to_json(xml_string):
    root = ET.fromstring(xml_string)
    result = {root.tag: xml_to_dict(root)}
    return json.dumps(result, indent=2)

def extract_relevant_data(data):
    li_id = data["Response"]["Product"]["product_id"]
    name = data["Response"]["Product"]["name"]


    
    moa = {
        'primary': [],
        'secondary': []
    }
    moas_data = data['Response']['MOAs']
    if moas_data:
        moas_data = moas_data['MOA']
        if isinstance(moas_data, list):
            for i in moas_data:
                if i.get("is_primary", "True")=="True":
                    moa['primary'].append(i.get("display_name"))
                else:
                    moa['secondary'].append(i.get("display_name"))
        elif isinstance(moas_data, dict):
            if moas_data.get("is_primary", "True")=="True":
                    moa['primary'].append(moas_data.get("display_name"))
            else:
                moa['secondary'].append(moas_data.get("display_name"))

    return {
        "li_id": li_id,
        "name": name,
        "primary_moa": moa['primary'],
        "secondary_moa": moa['secondary']
    }

def get_product_id_map(trial_data):
    product_map = {}
    for i in trial_data:
        try:
            if i:
                primary_products = i.get("primary_product")
                if primary_products:
                    for product in primary_products:
                        if product.get("primary_intervention")=="yes":
                            product_map.update(
                                {product.get("LI_id") : product.get("name")}
                            )
        except Exception as e:
            logger.error(f"Error getting data from trials: {e}")
    return product_map



# Main Lambda Handler
def main():
    args = parser.parse_args()
    logger.info(f"Starting update to DynamoDB since day {args.days}...")

    timestamp = get_yesterday_timestamp(args.days)
    updated_ids = get_updated_ids(timestamp)

    logger.info(f"Total updated IDs: {len(updated_ids)}")

    if not updated_ids:
        return {"status": "No updates found."}

    collected_results = []
    skipped = []
    product_id_map = {}

    for count, id in tqdm.tqdm(enumerate(updated_ids)):
        try:
            result = get_trial_data(id)
            if result:
                collected_results.append(result)
            else:
                skipped.append(id)
            
            # Write in batches of 50
            if count % 50 == 0 and count != 0 and collected_results:
                with onco_table.batch_writer(overwrite_by_pkeys=[ONCO_PARTITION_KEY]) as batch:
                    for item in collected_results:
                        batch.put_item(Item=item)
                logger.info(f"Batch written at count {count}")
                
                product_id_map.update(get_product_id_map(collected_results))
                collected_results = []
                time.sleep(20)

        except Exception as e:
            logger.error(f"Error processing ID {id}: {e}")

    # Final flush
    if collected_results:
        with onco_table.batch_writer(overwrite_by_pkeys=[ONCO_PARTITION_KEY]) as batch:
            for item in collected_results:
                batch.put_item(Item=item)
        product_id_map.update(get_product_id_map(collected_results))
        logger.info("Final batch write completed for Onco Trial Data.")

    logger.info({
        "total_updated": len(updated_ids),
        "skipped": len(skipped)}
    )




    logger.info(f"Total Updated Product IDs: {len(product_id_map)}")
    
    #### running code for updating product table 
    updated_keys = list(product_id_map.keys())
    request_keys = [{'li_id': sid} for sid in updated_keys]

    response = dynamodb.batch_get_item(
        RequestItems={
            product_table.name: {
                'Keys': request_keys
            }
        }
    )
    items = response['Responses'].get(product_table.name, [])
    existing_ids = [item['li_id'] for item in items]
    new_ids = list(set(updated_keys) - set(existing_ids))

    logger.info(f"Length of new product IDs: {len(new_ids)}")

    if new_ids:
        collected_results = []
        for count, id in enumerate(new_ids):
            try:
                data = get_product_data(id)
                data = json.loads(parse_xml_to_json(data))
                collected_results.append(extract_relevant_data(data))
            except Exception as e:
                logger.error(f"Error processing product ID: {id}\nError:{e}")
            
            if count % 20 == 0 and count != 0 and collected_results:
                with product_table.batch_writer(overwrite_by_pkeys=[PRODUCT_PARTITION_KEY]) as batch:
                    for item in collected_results:
                        batch.put_item(Item=item)
                logger.info(f"Product Batch written at count {count}")
                collected_results = []
                time.sleep(20)

        # final flush
        if collected_results:
            with product_table.batch_writer(overwrite_by_pkeys=[PRODUCT_PARTITION_KEY]) as batch:
                for item in collected_results:
                    batch.put_item(Item=item)
            logger.info("Final batch write completed for Product MOA Data.")
            collected_results = []


main()