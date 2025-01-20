import json
import random
import os
import time
from curl_cffi import requests
from bs4 import BeautifulSoup
import re
import concurrent.futures
from threading import Lock

with open('proxies.txt', 'r') as file:
    proxies = [line.strip() for line in file.readlines()]

json_file_path = "product_urls.json"
completed_pages_file = "completed_pages.json"
combined_json_file = 'combined.json'

lock = Lock()

def load_completed_product_ids():
    completed_product_ids = set()
    if os.path.exists(combined_json_file):
        with open(combined_json_file, 'r', encoding='utf-8') as json_file:
            try:
                data = json.load(json_file)
                for product_id in data:
                    completed_product_ids.add(product_id)
            except json.JSONDecodeError:
                pass
    return completed_product_ids

def make_request_with_retries(url, max_retries=5, wait_time=30):
    retries = 0
    session = requests.Session()

    while retries < max_retries:
        proxy = random.choice(proxies)
        proxy_scheme = "https" if "https://" in proxy else "http"
        proxies_dict = {
            "http": f"{proxy_scheme}://{proxy}",
            "https": f"{proxy_scheme}://{proxy}"
        }

        session.proxies = proxies_dict

        try:
            response = session.get(url, timeout=30, 
                                   headers={'User-Agent': random.choice(["chrome119", "chrome120", "safari", "chrome", "safari_ios"])})
            response.raise_for_status()
            return response

        except requests.exceptions.Timeout:
            pass
        except requests.exceptions.RequestException:
            pass

        retries += 1
        if retries < max_retries:
            time.sleep(2)
        else:
            time.sleep(wait_time)
            retries = 0

    raise Exception(f"Failed to retrieve {url} after {max_retries} retries.")

def extract_discover_json(url, product_id):
    try:
        r = make_request_with_retries(url)
        
        if r is None:
            return None

        soup = BeautifulSoup(r.text, 'html.parser')
        script_tag = soup.find('script', type="application/discover+json")
        
        if script_tag:
            json_data = script_tag.string.strip()
            data = json.loads(json_data)
            product_data = data['mfe-orchestrator']['props']['apolloCache'][f'ProductType:{product_id}']
            
            filtered_product_data = {
                "id": product_data.get("id"),
                "title": product_data.get("title"),
                "description": product_data.get("description"),
                "bulkBuyLimit": product_data.get("bulkBuyLimit"),
                "status": product_data.get("status"),
                "price": {
                    "actual": product_data.get("price", {}).get("actual"),
                    "unitPrice": product_data.get("price", {}).get("unitPrice"),
                    "unitOfMeasure": product_data.get("price", {}).get("unitOfMeasure")
                }
            }

            apollo_cache = data['mfe-orchestrator']['props']['apolloCache']
            first_promotion_type = next((value for key, value in apollo_cache.items() if key.startswith('PromotionType:')), None)

            if first_promotion_type:
                promotion_id = first_promotion_type['id']
                promotion_data = apollo_cache.get(f'PromotionType:{promotion_id}')
                
                filtered_promotion_data = {
                    "id": promotion_data.get("id"),
                    "startDate": promotion_data.get("startDate"),
                    "endDate": promotion_data.get("endDate"),
                    "description": promotion_data.get("description"),
                    "unitSellingInfo": promotion_data.get("unitSellingInfo")
                } if promotion_data else None
            else:
                filtered_promotion_data = None

            output_data = {
                str(product_id): {
                    "product_data": filtered_product_data,
                    "promotion_data": filtered_promotion_data
                }
            }

            return output_data

    except Exception:
        return None

def process_product_url(product_url, completed_product_ids):
    product_id = re.search(r'/products/(\d+)', product_url).group(1)

    if product_id in completed_product_ids:
        return None

    delay = random.uniform(0, 3)
    time.sleep(delay)

    json_data = extract_discover_json(f"https://www.tesco.com{product_url}", product_id)

    return json_data

def load_existing_data():
    if os.path.exists(combined_json_file):
        with open(combined_json_file, 'r', encoding='utf-8') as json_file:
            try:
                return json.load(json_file)
            except json.JSONDecodeError:
                return {}
    return {}

def save_data_to_json(data):
    with open(combined_json_file, 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, indent=4, ensure_ascii=False)

def main():
    with open('product_links.json', 'r') as file:
        product_urls = json.load(file)

    completed_product_ids = load_completed_product_ids()
    existing_data = load_existing_data()

    max_workers = 1
    processed_count = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_product_url, url, completed_product_ids): url for url in product_urls}

        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                if result:
                    existing_data.update(result)
                    processed_count += 1

                    if processed_count % 100 == 0:
                        save_data_to_json(existing_data)

            except Exception:
                pass

    save_data_to_json(existing_data)

if __name__ == "__main__":
    main()
