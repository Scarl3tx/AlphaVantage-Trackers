import os
from dotenv import load_dotenv
import json
import requests
from datetime import datetime
import time
import pandas as pd
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_PATH = PROJECT_ROOT / "config"
RAW_PATH = PROJECT_ROOT / "data" / "raw"
COMMODITIES_PATH = RAW_PATH / "commodities"

ENV_PATH = PROJECT_ROOT / "alphavantage_api.env"
load_dotenv(dotenv_path=ENV_PATH)
API_KEY = os.getenv("ALPHA_VANTAGE_KEY")

if not API_KEY:
    raise ValueError(f"❌ Critical Pipeline Failure: Could not locate or read the API key")
else:
    print(f"✅ Success: API Key loaded securely.")

def extract_raw_commodities(asset_name: str, asset_config: dict) -> dict:
    base_url = "https://alphavantage.co/query?"
    params = {
        "function": asset_config["function"],
        "apikey": API_KEY
    }
    # 2. Dynamically inject the symbol parameter ONLY if the URL template requires it
    if asset_config.get("symbol_param"):
        params[asset_config["symbol_param"]] = asset_name

    # 3. Dynamically inject any custom endpoint parameters (like interval or outputsize)
    if "additional_params" in asset_config:
        params.update(asset_config["additional_params"])

    print(f"🔄 Fetching data via tailored URL parameters for {asset_name}...")

    response = requests.get(base_url, params=params)
    data = response.json()

    # 4. Data Quality Firewall: Check for the rate limit warning you received
    if "Information" in data:
        print(f"⚠️ RATE LIMIT DETECTED for {asset_name}: {data['Information']}")
        return {}
    if "Error Message" in data:
        print(f"❌ API Error for {asset_name}: {data['Error Message']}")
        return {}
        
    return data

def save_raw_to_disk(asset_name: str, data: dict) -> bool:
    if not data:
        return False
    
    # 1. Establish directory pathing for parquet storage
    commodities_dir = COMMODITIES_PATH / asset_name
    os.makedirs(commodities_dir, exist_ok=True)

    # 2. Generate hourly filename pathing
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H")
    file_path = commodities_dir / f"{timestamp}_{asset_name}_raw.pq"

    # 3. IF NOT ALREADY EXIST FIREWALL
    # Skips execution cleanly if the file was pulled earlier this hour
    if file_path.exists():
        print(f"⏭️ File already exists for {asset_name} this hour, skipping save: {file_path}")
        return True
    
    # 4. Save to Disk
    try:
        df = pd.DataFrame(data)
        df.to_parquet(file_path, engine="pyarrow", index=False)
        print(f"💾 Saved clean raw Parquet file to: {file_path}")
        return True
    except Exception as e:
        print(f"❌ Failed to convert and save {asset_name} data to Parquet: {e}")
        return False
    
def run_alphavantage_commodities_bronze(configs: dict) -> bool:
    print(f"🚀 Initializing commodities ingestion cycle...")
    success = True
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H")

    for commodities_name, commodities_config in configs.items():
        expected_file = COMMODITIES_PATH / commodities_name / f"{timestamp}_{commodities_name}_raw.pq"
        if expected_file.exists():
            print(f"⏭️ [API SKIPPED] Data for {commodities_name} already pulled this hour.")
            continue
        raw_payload = extract_raw_commodities(commodities_name, commodities_config)

        if not raw_payload:
            success = False
            continue

        if not save_raw_to_disk(commodities_name, raw_payload):
            success = False
            continue
        # 5. THE RATE LIMIT SOLVER: Force the script to rest for 15 seconds
        # This keeps the free key under the maximum calls threshold safely
        print("Sleeping for 15 seconds to respect free API rate limits...")
        time.sleep(15)
        
    print("✅ Full commodities extraction cycle finalized.\n")
    return success
if __name__ == "__main__":
    commodities_config_path = CONFIG_PATH / "alphavantage_commodities_config.json"

    try:
        with open(commodities_config_path, "r", encoding="utf-8") as file:
            commodities_config_data = json.load(file)
        local_configs = commodities_config_data.get("commodities", {})

        if not local_configs:
            print("❌ No commodities configurations found.")
        else:
            run_alphavantage_commodities_bronze(configs=local_configs)

    except FileNotFoundError:
        print(f"❌ Configuration file not found: {commodities_config_path}")
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON configuration: {e}")