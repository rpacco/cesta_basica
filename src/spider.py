import httpx
import json
import pandas as pd
import re
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)

def fetch_product_data(payload, logger):
    url = "https://api.vendas.gpa.digital/pa/v3/products/ecom/skuLivePrice"
    querystring = {"storeId": "461", "sellType": "null", "sortBy": "relevance", "isClienteMais": "false"}
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        response = httpx.post(url, params=querystring, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()['content']
    except httpx.HTTPStatusError as e:
        logger.log_text(f"HTTP error occurred while fetching data: {str(e)}", severity="ERROR")
        raise

def get_category(name):
    with open('src/items_map.json', 'r') as file:
        alimentos_mapping = json.load(file)
    for key in alimentos_mapping.keys():
        if key.lower() in name.lower():
            return alimentos_mapping[key]
    return None

def extract_weight(text):
    pattern = r'(\d+([,.]\d+)?)\s*(k?g|g|m?l|l)(?!\w)'
    match = re.search(pattern, text, re.IGNORECASE)
    
    if match:
        value = float(match.group(1).replace(',', '.'))
        unit = match.group(3).lower()
        if unit == 'g':
            return value / 1000, 'kg'
        elif unit == 'ml':
            return value / 1000, 'l'
        return value, unit
    else:
        return None, None

def wrangle_data(items_path, logger):
    df = pd.read_csv(items_path, delimiter=';')
    skus = []
    for sku_list in df['sku']:
        skus.extend(json.loads(sku_list))
    # Remove duplicates if necessary
    skus = list(set(skus))

    try:
        data_raw = fetch_product_data(skus, logger)
        data = pd.DataFrame(data_raw)
        today = datetime.today().date()
        features = ['name', 'price']
        data = data[features]
        data[['value', 'unit']] = data['name'].apply(lambda x: extract_weight(x)).apply(pd.Series)
        data['category'] = data['name'].map(get_category)
        data = data.merge(df, left_on='category', right_on='alimentos', suffixes=('_source', '_dieese'))
        data.drop(columns=['alimentos', 'sku'], inplace=True)
        data['adjusted_price'] = (data['value_dieese'] / data['value']) * data['price']
        data['date'] = today
        return data
    except Exception as e:
        logger.log_struct(
            {
                "message": "Error occurred while wrangling data",
                "error": str(e)
            },
            severity="ERROR"
        )
        raise