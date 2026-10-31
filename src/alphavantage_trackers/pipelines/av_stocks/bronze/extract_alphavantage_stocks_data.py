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
    STOCKS_PATH
)
from alphavantage_trackers.logging_config import get_pipeline_logger
logger = get_pipeline_logger("stocks")

from alphavantage_trackers.api.alphavantage_rate_limiter import av_rate_limiter

load_dotenv(dotenv_path=ENV_PATH)
API_KEY = os.getenv("ALPHA_VANTAGE_KEY")

if not API_KEY:
    logger.error(f"❌ Critical Pipeline Failure: Could not locate or read the API key")
    raise ValueError(f"❌ Critical Pipeline Failure: Could not locate or read the API key")
else:
    logger.info(f"✅ Success: API Key loaded securely.")

def extract_raw_stock(asset_name: str, asset_config: dict) -> dict:
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
        print(f"❌ No data to save for {asset_name}.")
        return False
    
    # 1. Establish directory pathing for parquet storage
    stocks_dir = STOCKS_PATH / asset_name
    os.makedirs(stocks_dir, exist_ok=True)

    # 2. Generate hourly filename pathing
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H")
    file_path = stocks_dir / f"{timestamp}_{asset_name}_raw.pq"

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
    
def run_alphavantage_stock_bronze(configs: dict) -> bool:
    logger.info(f"🚀 Initializing stock ingestion cycle...")
    success = True
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H")

    for stock_name, stock_config in configs.items():
        expected_file = STOCKS_PATH / stock_name /f"{timestamp}_{stock_name}_raw.pq"
        if expected_file.exists():
            logger.info(f"⏭️ [API SKIPPED] Data for {stock_name} already pulled this hour.")
            continue
        raw_payload = extract_raw_stock(stock_name, stock_config)

        if not raw_payload:
            success = False
            continue

        if not save_raw_to_disk(stock_name, raw_payload):
            success = False
            continue
        
    logger.info("✅ Full stock extraction cycle finalized.")
    return success
if __name__ == "__main__":
    stock_config_path = CONFIG_PATH / "alphavantage_stock_config.json"

    try:
        with open(stock_config_path, "r", encoding="utf-8") as file:
            stock_config_data = json.load(file)
        local_configs = stock_config_data.get("stocks", {})

        if not local_configs:
            print("❌ No stock configurations found.")
        else:
            run_alphavantage_stock_bronze(configs=local_configs)

    except FileNotFoundError:
        print(f"❌ Configuration file not found: {stock_config_path}")
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON configuration: {e}")