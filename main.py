from src.spider import wrangle_data
from src.gen_texts import cb_daily_text
from utils.bq_updater import save_to_bigquery
import httpx
import os
from google.cloud import logging as cloud_logging

# Initialize Google Cloud Logging client
client = cloud_logging.Client()
logger = client.logger('product-data-logger')


def main_run(request):
    data = wrangle_data('src/items.csv', logger)
    PROJECT_ID = os.environ.get('PROJECT_ID')
    DATASET_ID = os.environ.get('DATASET_ID')
    TABLE_ID = os.environ.get('TABLE_ID')
    try:
        job = save_to_bigquery(data, PROJECT_ID, DATASET_ID, TABLE_ID, logger)
        return job
    except httpx.HTTPStatusError as e:
        print(f"HTTP error occurred: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")