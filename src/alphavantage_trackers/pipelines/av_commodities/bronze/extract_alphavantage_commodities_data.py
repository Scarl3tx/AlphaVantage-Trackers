import os
from dotenv import load_dotenv
import json
import requests
from datetime import datetime
import time
import pandas as pd
from pathlib import Path
from typing import Optional

from alphavantage_trackers.paths import (
    CONFIG_PATH,
    ENV_PATH,
    COMMODITIES_PATH
)
from alphavantage_trackers.api.alphavantage_rate_limiter import av_rate_limiter
from alphavantage_trackers.logging_config import get_pipeline_logger

logger = get_pipeline_logger("commodities")

load_dotenv(dotenv_path=ENV_PATH)
API_KEY = os.getenv("ALPHA_VANTAGE_KEY")

if not API_KEY:
    logger.error(f"❌ Critical Pipeline Failure: Could not locate or read the API key")
    raise ValueError(f"❌ Critical Pipeline Failure: Could not locate or read the API key")
else:
    logger.info(f"✅ Success: API Key loaded securely.")

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

    logger.info("Waiting for API request slot for %s...", asset_name)
    av_rate_limiter.acquire()
    logger.info("Sending API request for %s...", asset_name)

    try:
        response = requests.get(base_url, params=params,timeout=30)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException:
        logger.exception(f"HTTP request failed for {asset_name}")
        return {}

    except ValueError:
        logger.exception(f"Invalid JSON response for {asset_name}")
        return {}
    
    # 4. Data Quality Firewall: Check for the rate limit warning you received
    if "Information" in data:
        logger.warning(f"⚠️ RATE LIMIT DETECTED for {asset_name}: {data['Information']}")
        return {}
    if "Error Message" in data:
        logger.error(f"❌ API Error for {asset_name}: {data['Error Message']}")
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
        logger.info(f"⏭️ File already exists for {asset_name} this hour, skipping save: {file_path}")
        return True
    
    # 4. Save to Disk
    try:
        df = pd.DataFrame(data)
        df.to_parquet(file_path, engine="pyarrow", index=False)
        logger.info(f"💾 Saved clean raw Parquet file to: {file_path}")
        return True
    except Exception as e:
        logger.exception(f"❌ Failed to convert and save {asset_name} data to Parquet: {e}")
        return False
    
def run_alphavantage_commodities_bronze(configs: dict) -> bool:
    logger.info(f"🚀 Initializing commodities ingestion cycle...")
    success = True
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H")

    for commodities_name, commodities_config in configs.items():
        expected_file = COMMODITIES_PATH / commodities_name / f"{timestamp}_{commodities_name}_raw.pq"
        if expected_file.exists():
            logger.info(f"⏭️ [API SKIPPED] Data for {commodities_name} already pulled this hour.")
            continue
        raw_payload = extract_raw_commodities(commodities_name, commodities_config)

        if not raw_payload:
            success = False
            continue

        if not save_raw_to_disk(commodities_name, raw_payload):
            success = False
            continue
        
    if success:
        logger.info("Commodities extraction completed successfully")
    else:
        logger.warning("Commodities extraction completed with failures")
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