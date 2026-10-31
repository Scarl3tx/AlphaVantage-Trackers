import json
from pathlib import Path

from alphavantage_trackers.pipelines.av_commodities.bronze.extract_alphavantage_commodities_data import (
    run_alphavantage_commodities_bronze
)

from alphavantage_trackers.pipelines.av_commodities.silver.silver_transform_alphavantage_commodities import (
    run_alphavantage_commodities_silver
)

from alphavantage_trackers.paths import (
    CONFIG_PATH,
)


def run_commodities_pipeline():

    print("\n========== COMMODITIES PIPELINE ==========")

    # -------------------------
    # LOAD CONFIGURATION
    # -------------------------

    try:
        with open(
            CONFIG_PATH / "alphavantage_commodities_config.json",
            "r",
            encoding="utf-8"
        ) as file:
            config_data = json.load(file)

        commodities_config = config_data.get("commodities", {})

        if not commodities_config:
            print("[COMMODITIES] No configurations found.")
            return False

    except (FileNotFoundError, json.JSONDecodeError) as exc:
        print(f"[COMMODITIES] Configuration error: {exc}")
        return False

    # -------------------------
    # BRONZE
    # -------------------------

    print("[COMMODITIES] Starting Bronze")

    bronze_success = run_alphavantage_commodities_bronze(
        configs=commodities_config
    )

    if not bronze_success:
        print("[COMMODITIES] Bronze failed. Skipping Silver.")
        return False

    print("[COMMODITIES] Bronze completed successfully")

    # -------------------------
    # SILVER
    # -------------------------

    print("[COMMODITIES] Starting Silver")

    try:
        with open(
            CONFIG_PATH / "alphavantage_commodities_schemas.json",
            "r",
            encoding="utf-8"
        ) as file:
            schema_config = json.load(file)

        if not schema_config:
            print("[COMMODITIES] No schema configurations found.")
            return False
        
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        print(f"[COMMODITIES] Configuration error: {exc}")
        return False

    silver_success = run_alphavantage_commodities_silver(
        configs=commodities_config,
        asset_schemas=schema_config
    )

    if not silver_success:
        print("[COMMODITIES] Silver failed.")
        return False

    print("[COMMODITIES] Silver completed successfully")

    print("========== COMMODITIES PIPELINE COMPLETE ==========\n")

    return True