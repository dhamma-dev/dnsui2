import os
import json
import logging
import threading
import json
from flask import Flask, jsonify, request, send_from_directory
import ingestion_service

# --- CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
CONFIG_FILE = 'config.json'
DB_FILE = 'dns_data.db'
app = Flask(__name__, static_folder='.', static_url_path='')
app.config['DB_FILE'] = DB_FILE

# Global variable to track ingestion status
ingestion_status = {"running": False}

# --- HELPER FUNCTIONS ---
def get_config():
    """Loads configuration from the JSON file."""
    if not os.path.exists(CONFIG_FILE):
        return {}
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)

def save_config(config_data):
    """Saves configuration to the JSON file."""
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config_data, f, indent=4)
        
def run_ingestion_thread():
    """Wrapper function to run ingestion in a separate thread."""
    if ingestion_status["running"]:
        logging.warning("Ingestion is already in progress. Skipping new request.")
        return

    ingestion_status["running"] = True
    logging.info("Starting ingestion thread.")
    try:
        config = get_config()
        if not all([config.get('API_BASE_URL'), config.get('ORG_ID'), config.get('API_TOKEN')]):
             raise ValueError("API configuration is incomplete.")
        ingestion_service.run_full_ingestion(config)
        # Update last synced time after successful ingestion
        config['LAST_SYNCED_UTC'] = ingestion_service.get_current_utc_iso()
        save_config(config)
    except Exception as e:
        logging.error(f"Ingestion thread failed: {e}", exc_info=True)
    finally:
        ingestion_status["running"] = False
        logging.info("Ingestion thread finished.")

# --- FLASK API ENDPOINTS ---
@app.route('/')
def index():
    """Serves the main dashboard HTML file."""
    return send_from_directory('.', 'dns_dashboard.html')

@app.route('/api/config', methods=['GET', 'POST'])
def handle_config():
    """Handles loading and saving of API configuration."""
    if request.method == 'POST':
        new_config = request.json
        config = get_config()
        config['API_BASE_URL'] = new_config.get('API_BASE_URL')
        config['ORG_ID'] = new_config.get('ORG_ID')
        # Only update token if a new one is provided
        if new_config.get('API_TOKEN'):
            config['API_TOKEN'] = new_config.get('API_TOKEN')
        save_config(config)
        return jsonify({"status": "success", "message": "Configuration saved."})
    
    config = get_config()
    # Mask the token before sending to the client
    if 'API_TOKEN' in config:
        config['API_TOKEN'] = '********'
    return jsonify(config)

@app.route('/api/data', methods=['GET'])
def get_data():
    """Retrieves transformed DNS records from the database, with optional time filters."""
    try:
        start_time_str = request.args.get('start')
        end_time_str = request.args.get('end')

        start_time = int(start_time_str) if start_time_str else None
        end_time = int(end_time_str) if end_time_str else None

        records = ingestion_service.get_records_from_db(start_time, end_time)
        return jsonify(records)
    except ValueError:
        return jsonify({"error": "Invalid timestamp format. Please use Unix timestamps."}), 400
    except Exception as e:
        logging.error(f"Failed to retrieve data from database: {e}", exc_info=True)
        return jsonify({"error": "Could not retrieve data from the database."}), 500

@app.route('/api/run-ingestion', methods=['POST'])
def run_ingestion_endpoint():
    """Triggers the data ingestion process."""
    if ingestion_status["running"]:
        return jsonify({"status": "error", "error": "An ingestion process is already running."}), 429 # Too Many Requests

    logging.info("Manual ingestion triggered via API.")
    # Run ingestion in a background thread to not block the API response
    thread = threading.Thread(target=run_ingestion_thread)
    thread.start()
    return jsonify({"status": "success", "message": "Ingestion process started."})

if __name__ == '__main__':
    logging.info("Starting Flask server.")
    # Initialize the database on startup if it doesn't exist
    ingestion_service.initialize_database()
    # use_reloader=False is important to prevent database locking issues with multiple workers
    app.run(debug=True, port=5001, use_reloader=False)

