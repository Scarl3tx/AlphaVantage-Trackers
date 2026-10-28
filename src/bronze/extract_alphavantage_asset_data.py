import os
from dotenv import load_dotenv
import json
import requests
from datetime import datetime
import time
import pandas as pd
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
env_path = project_root / "alpha_vantage_api.env"
load_dotenv(dotenv_path=env_path)
API_KEY = os.getenv("ALPHA_VANTAGE_KEY")

if not API_KEY:
    raise ValueError(f"❌ Critical Pipeline Failure: Could not locate or read the API key")
else:
    print(f"✅ Success: API Key loaded securely.")

def extract_raw_asset(asset_name: str, asset_config: dict) -> dict:
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

def save_raw_to_disk(asset_name: str, data: dict):
    if not data:
        return
    
    # 1. Establish directory pathing for parquet storage
    raw_dir = project_root / "data" / "raw" / asset_name
    os.makedirs(raw_dir, exist_ok=True)

    # 2. Generate hourly filename pathing
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H")
    file_path = os.path.join(raw_dir, f"{asset_name}_raw_{timestamp}.pq")

    # 3. IF NOT ALREADY EXIST FIREWALL
    # Skips execution cleanly if the file was pulled earlier this hour
    if os.path.exists(file_path):
        print(f"⏭️ File already exists for {asset_name} this hour, skipping save: {file_path}")
        return
    
    # 4. Save to Disk
    try:
        df = pd.DataFrame(data)
        df.to_parquet(file_path, engine="pyarrow", index=False)
        print(f"💾 Saved clean raw Parquet file to: {file_path}")

    except Exception as e:
        print(f"❌ Failed to convert and save {asset_name} data to Parquet: {e}")

def run_alphavantage_bronze(configs: dict):
    print(f"🚀 Initializing asset ingestion cycle...")

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H")
    assets_to_process = configs.get("assets", {})

    for asset_name, asset_config in assets_to_process.items():
        expected_file = project_root / "data" / "raw" / asset_name / f"{asset_name}_raw_{timestamp}.pq"
        if expected_file.exists():
            print(f"⏭️ [API SKIPPED] Data for {asset_name} already pulled this hour.")
            continue
        raw_payload = extract_raw_asset(asset_name, asset_config)
        save_raw_to_disk(asset_name, raw_payload)
        # 5. THE RATE LIMIT SOLVER: Force the script to rest for 15 seconds
        # This keeps the free key under the maximum calls threshold safely
        print("Sleeping for 15 seconds to respect free API rate limits...")
        time.sleep(15)
        
    print("✅ Full extraction cycle finalized.")

if __name__ == "__main__":
    config_path = project_root / "config.json"
    try:
        with open(config_path, "r") as config_file:
            config_data = json.load(config_file)
        local_configs = config_data.get("assets", {})
        run_alphavantage_bronze(configs=local_configs)
    except FileNotFoundError:
        print(f"❌ Cannot run standalone: '{config_path}' not found.")