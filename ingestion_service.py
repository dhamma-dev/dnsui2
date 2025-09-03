import sqlite3
import requests
import logging
import time
import json
from datetime import datetime, timezone

DB_FILE = 'dns_data.db'

# --- DATABASE FUNCTIONS ---
def initialize_database():
    """Creates the necessary tables in the SQLite database if they don't exist."""
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS appliances (
                guid TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                location TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dns_records (
                timestamp INTEGER NOT NULL,
                appliance_name TEXT,
                appliance_guid TEXT,
                target_domain TEXT,
                dns_server TEXT,
                record_type TEXT,
                response_code TEXT,
                resolution_time_ms INTEGER,
                resolved_ips TEXT,
                PRIMARY KEY (timestamp, appliance_guid, target_domain, dns_server, record_type)
            )
        ''')
        conn.commit()

def get_all_records_from_db():
    """Fetches all DNS records and returns them as a list of dictionaries."""
    with sqlite3.connect(DB_FILE) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM dns_records ORDER BY timestamp DESC")
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

# --- API FETCHING FUNCTIONS ---
def fetch_api_data(url, headers, params):
    """Generic function to fetch data from the API with error handling."""
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed for {url}: {e}")
        raise

def fetch_appliance_data(base_url, org_id, token):
    """Fetches appliance metadata."""
    url = f"{base_url}/appliance"
    headers = {'Authorization': f'Token {token}'}
    params = {'orgId': org_id}
    return fetch_api_data(url, headers, params)

def fetch_dns_data(base_url, org_id, token, start_time, end_time):
    """Fetches raw DNS resolution data, handling pagination."""
    all_data = []
    page = 1
    url = f"{base_url}/dns/webPath/data"
    headers = {'Authorization': f'Token {token}'}
    
    while True:
        params = {
            'orgId': org_id,
            'from': int(start_time),
            'to': int(end_time),
            'limit': 10,
            'page': page
        }
        try:
            logging.info(f"Fetching page {page} of DNS data...")
            page_data = fetch_api_data(url, headers, params)
            if not page_data:
                logging.info("No more pages to fetch.")
                break
            all_data.extend(page_data)
            logging.info(f"Successfully fetched page {page} with {len(page_data)} items.")
            page += 1
            time.sleep(1) # Be respectful to the API
        except Exception as e:
            logging.error(f"Failed to fetch page {page}: {e}")
            break # Stop if a page fails
            
    return all_data

# --- DATA TRANSFORMATION & STORAGE ---
def transform_and_store_data(raw_dns_data, appliance_map, window_start_time):
    """Transforms nested API data into a flat structure and stores it in the DB."""
    flat_records = []
    for group in raw_dns_data:
        appliance_guid = group.get('applianceGuid')
        appliance_name = appliance_map.get(appliance_guid, 'Unknown Appliance')
        
        for series_item in group.get('series', []):
            timestamp = series_item.get('timestamp') // 1000 
            
            for data_item in series_item.get('data', []):
                # CRITICAL FIX: Skip if data_item is null or not a dictionary
                if not isinstance(data_item, dict):
                    continue

                dns_server = data_item.get('dnsServer')
                
                for record_type in ['A', 'AAAA', 'CNAME']:
                    record_data = data_item.get(record_type)
                    if isinstance(record_data, dict):
                        flat_records.append({
                            'timestamp': timestamp,
                            'appliance_name': appliance_name,
                            'appliance_guid': appliance_guid,
                            'target_domain': group.get('targetDomain'),
                            'dns_server': dns_server,
                            'record_type': record_type,
                            'response_code': record_data.get('responseCode'),
                            'resolution_time_ms': record_data.get('resolutionTime'),
                            'resolved_ips': json.dumps(record_data.get('resolvedIp', []))
                        })

    if not flat_records:
        logging.warning("No records were transformed. Check the raw API data.")
        return

    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        logging.info(f"Deleting existing records from database within time window (>= {window_start_time})...")
        cursor.execute("DELETE FROM dns_records WHERE timestamp >= ?", (window_start_time,))
        logging.info(f"Deleted {cursor.rowcount} old records.")

        logging.info(f"Inserting {len(flat_records)} new records...")
        # Use INSERT OR IGNORE to gracefully handle any duplicates from the API
        cursor.executemany('''
            INSERT OR IGNORE INTO dns_records (timestamp, appliance_name, appliance_guid, target_domain, dns_server, record_type, response_code, resolution_time_ms, resolved_ips)
            VALUES (:timestamp, :appliance_name, :appliance_guid, :target_domain, :dns_server, :record_type, :response_code, :resolution_time_ms, :resolved_ips)
        ''', flat_records)
        conn.commit()
        logging.info(f"Successfully inserted/updated {cursor.rowcount} records.")

# --- MAIN INGESTION WORKFLOW ---
def run_full_ingestion(config):
    """The main function to orchestrate the entire data fetch and store process."""
    base_url = config['API_BASE_URL']
    org_id = config['ORG_ID']
    token = config['API_TOKEN']

    initialize_database()

    logging.info("Fetching appliance data...")
    appliances = fetch_appliance_data(base_url, org_id, token)
    appliance_map = {app['guid']: app['name'] for app in appliances}
    logging.info(f"Successfully fetched metadata for {len(appliance_map)} appliances.")

    end_time = int(time.time())
    start_time = end_time - (24 * 60 * 60) # 24 hours ago
    logging.info(f"Starting DNS data fetch for window: {start_time} to {end_time}")
    raw_dns_data = fetch_dns_data(base_url, org_id, token, start_time, end_time)

    transform_and_store_data(raw_dns_data, appliance_map, start_time)
    logging.info("Ingestion process completed successfully.")
    
def get_current_utc_iso():
    """Returns the current time in UTC ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()

