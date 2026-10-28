import os
import sys
from pathlib import Path
import json

# =====================================================================
# 1. DEFINE GLOBAL BASE DIRECTORIES (At the very top of your script)
# =====================================================================
PROJECT_ROOT = Path(__file__).resolve().parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
SILVER_DATA_DIR = PROJECT_ROOT / "data" / "silver"
CONFIG_PATH = PROJECT_ROOT / "configs"
# 2. Layer Imports
# If your folder structure is: your_project/src/bronze_extraction/bronze_layer.py
from bronze.extract_alphavantage_asset_data import run_alphavantage_bronze
from silver.silver_transform import run_alphavantage_silver
from silver.silver_transform import load_json_file #prob shd move this to funcs

class Orchestrator():
    def __init__(self):
        self._registry: dict = {}

    def set_config(self, key: str, config_data: dict):
        """Dynamically registers any configuration object by a string key."""
        if not isinstance(config_data, dict):
            raise TypeError(f"Configuration data for '{key}' must be a dictionary.")
        self._registry[key] = config_data

    def _get_config(self, key: str) -> dict:
        """Internal helper to safely fetch configs with a clear error message."""
        if key not in self._registry:
            raise KeyError(f"🚨 Required configuration '{key}' has not been registered in the Orchestrator.")
        return self._registry[key]
    
    def run_bronze_layer(self) -> bool:
        print("\n=== [STAGE 1] BRONZE LAYER EXTRACTION ===")
        try:
            bronze_av_configs = self._get_config("av_configs")
            run_alphavantage_bronze(bronze_av_configs)
            return True
        except Exception as e:
            print(f"💥 Pipeline halted during Bronze Layer: {e}")
            return False
        
    def run_silver_layer(self):
        try:
            silver_av_configs = self._get_config("av_configs")
            schemas = self._get_config("av_asset_schemas")
            run_alphavantage_silver(silver_av_configs, schemas)
            return True

        except Exception as e:
            print(f"💥 Silver failed: {e}")
            return False
        
    def execute(self, required_stages: list[str]):
        """Verifies that all required configurations for the run are present upfront."""
        for stage in required_stages:
            if stage not in self._registry:
                raise ValueError(f"Cannot execute. Missing registered config for: '{stage}'")
        print("🚀 Starting End-to-End Data Pipeline...")
        # 2. RUN BRONZE LAYER
        bronze_success = self.run_bronze_layer()
        if not bronze_success:
            print("🚨 Pipeline stopped: Skipping Silver Layer due to Bronze failure.")
            return
        # It will only print if run_bronze_layer() returned True.
        print("✅ Bronze Layer extraction complete.")

        # 3. RUN SILVER LAYER
        silver_success = self.run_silver_layer()
        
        if not silver_success:
            print("🚨 Pipeline completed with Silver transformation errors.")
            return
        # It will only print if run_silver_layer() returned True.
        print("✅ Silver Layer transformations complete.")
        print("\n🏆 End-to-End Pipeline executed successfully!")
# =====================================================================
# 3. MAIN RUNNER FUNCTION (Where you define file-specific paths)
# =====================================================================
def execute_orchestrator():
    print("🚀 Initializing Pipeline Context Setup...")

    # Define the exact configuration file paths
    asset_config_path = CONFIG_PATH / "alphavantage_asset_config.json"
    asset_schema_path = CONFIG_PATH / "alphavantage_asset_schemas.json"
    orchestrator = Orchestrator()
    try:
        with open(asset_config_path, "r") as f:
            orchestrator.set_config("av_asset_configs", load_json_file(asset_config_path))
        with open(asset_schema_path, "r") as f:
            orchestrator.set_config("av_asset_schemas", load_json_file(asset_schema_path))
    except FileNotFoundError as e:
        print(f"❌ Context Setup Failed: {e.filename} is missing.")
        sys.exit(1)
    # Run the pipeline
    orchestrator.execute(required_stages=["av_asset_configs", "av_asset_schemas"])

if __name__ == "__main__":
    execute_orchestrator()