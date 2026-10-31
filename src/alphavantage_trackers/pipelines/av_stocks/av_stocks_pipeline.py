import json
from pathlib import Path

from alphavantage_trackers.pipelines.av_stocks.bronze.extract_alphavantage_stocks_data import (
    run_alphavantage_stock_bronze
)

from alphavantage_trackers.pipelines.av_stocks.silver.silver_transform_alphavantage_stocks import (
    run_alphavantage_stocks_silver
)

from alphavantage_trackers.paths import (
    CONFIG_PATH,
)

def run_stocks_pipeline():

    print("\n========== COMMODITIES PIPELINE ==========")

    # -------------------------
    # LOAD CONFIGURATION
    # -------------------------

    try:
        with open(
            CONFIG_PATH / "alphavantage_stock_config.json",
            "r",
            encoding="utf-8"
        ) as file:
            config_data = json.load(file)

        stocks_config = config_data.get("stocks", {})

        if not stocks_config:
            print("[STOCKS] No configurations found.")
            return False

    except (FileNotFoundError, json.JSONDecodeError) as exc:
        print(f"[STOCKS] Configuration error: {exc}")
        return False

    # -------------------------
    # BRONZE
    # -------------------------

    print("[STOCKS] Starting Bronze")

    bronze_success = run_alphavantage_stock_bronze(
        configs=stocks_config
    )

    if not bronze_success:
        print("[STOCKS] Bronze failed. Skipping Silver.")
        return False

    print("[STOCKS] Bronze completed successfully")

    # -------------------------
    # SILVER
    # -------------------------

    print("[STOCKS] Starting Silver")

    try:
        with open(
            CONFIG_PATH / "alphavantage_stock_schemas.json",
            "r",
            encoding="utf-8"
        ) as file:
            schema_config = json.load(file)

        if not schema_config:
            print("[STOCKS] No schema configurations found.")
            return False
        
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        print(f"[STOCKS] Configuration error: {exc}")
        return False

    silver_success = run_alphavantage_stocks_silver(
        configs=stocks_config,
        asset_schemas=schema_config
    )

    if not silver_success:
        print("[STOCKS] Silver failed.")
        return False

    print("[STOCKS] Silver completed successfully")

    print("========== STOCKS PIPELINE COMPLETE ==========\n")

    return True