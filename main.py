import os
import sys
from pathlib import Path
import importlib


# =====================================================================
# 1. DEFINE GLOBAL BASE DIRECTORIES (At the very top of your script)
# =====================================================================
PROJECT_ROOT = Path(__file__).resolve().parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
BRONZE_DIR = PROJECT_ROOT / "src" / "bronze"
SILVER_DATA_DIR = PROJECT_ROOT / "data" / "silver"
CONFIG_PATH = PROJECT_ROOT / "configs"

# 2. Layer Imports
# If your folder structure is: your_project/src/bronze_extraction/bronze_layer.py
from src.bronze.ingest import run_bronze_extraction
from silver.silver_transform import run_alphavantage_silver
from silver.silver_transform import load_json_file #prob shd move this to funcs

class Orchestrator():
    def __init__(self):
        self._registry: dict[str, dict] = {}

    def set_config(self, asset_label: str, config_type: str, config_data: dict):
        """
        Dynamically initializes the nested tree structure on the fly.
        Allows you to populate self._registry["stock"]["config"] dynamically.
        """
        if not isinstance(config_data, dict):
            raise TypeError("Configuration data must be a dictionary.")
            
        # 1. DYNAMIC INITIALIZATION: If "stock" doesn't exist yet, create its placeholders
        if asset_label not in self._registry:
            self._registry[asset_label] = {
                "config": {},
                "schema": {}
            }
            
        # 2. Assign data to the specific nested slot ("config" or "schema")
        self._registry[asset_label][config_type] = config_data
        print(f"✅ Dynamic Path Populated: self._registry['{asset_label}']['{config_type}']")


    def _get_config(self, asset_label: str, config_type: str) -> dict:
        """Helper to fetch configs from the dynamic nested array map safely."""
        try:
            return self._registry[asset_label][config_type]
        except KeyError:
            raise KeyError(f"🚨 Path self._registry['{asset_label}']['{config_type}'] has not been registered.")

    def run_bronze_layer(self) -> bool:
        print("\n=== [STAGE 1] BRONZE LAYER EXTRACTION ===")

        pipelines = [
            {
                "label": asset_label,
                "config": asset_package["config"]
            }
            for asset_label, asset_package in self._registry.items()
            if asset_package["config"]
        ]

        if not pipelines:
            print("❌ No active asset configurations registered.")
            return False

        try:
            status_report = run_bronze_extraction(pipelines)
            print(f"Bronze Status: {status_report}")
            return bool(status_report) and all(status_report.values())

        except Exception as e:
            print(f"💥 Bronze extraction failed: {e}")
            return False
        
    def run_silver_layer(self) -> bool:
        print("\n=== [STAGE 2] SILVER LAYER TRANSFORMATION ===")

        if not self._registry:
            print("❌ No configurations registered.")
            return False

        try:
            for asset_label, asset_package in self._registry.items():
                config = asset_package.get("config", {})
                schema = asset_package.get("schema", {})

                if not config or not schema:
                    print(
                        f"❌ '{asset_label}' is missing its config or schema."
                    )
                    return False

                # Each config JSON is expected to contain one top-level
                # key mapping asset names to their individual configurations.
                if len(config) != 1:
                    print(
                        f"❌ Expected exactly one top-level key in the "
                        f"'{asset_label}' config; found {list(config.keys())}."
                    )
                    return False

                asset_configs = next(iter(config.values()))

                print(f"\n🔄 Transforming '{asset_label}' assets...")

                run_alphavantage_silver(
                    configs={"assets": asset_configs},
                    asset_schemas=schema
                )

            print("\n✅ Silver layer execution completed.")
            return True
    
        except Exception as e:
            print(f"💥 Silver transformation failed: {e}")
            return False

        
    def execute(self):
        """Orchestrates pipeline steps natively across the compiled dynamic configuration state."""
        print("🚀 Starting End-to-End Data Pipeline...")
        # 1. RUN BRONZE LAYER
        if not self.run_bronze_layer():
            print("🚨 Pipeline stopped: Skipping Silver Layer due to Bronze failure.")
            return
        print("✅ Bronze Layer extraction complete.")

        # 2. RUN SILVER LAYER
        if not self.run_silver_layer():
            print("🚨 Pipeline completed with Silver transformation errors.")
            return
        print("\n🏆 End-to-End Pipeline executed successfully!")

# =====================================================================
# 3. MAIN RUNNER FUNCTION
# =====================================================================
def execute_orchestrator():
    print("🚀 Initializing Pipeline Context Setup...")

    stock_config_path = CONFIG_PATH / "alphavantage_stock_config.json"
    stock_schema_path = CONFIG_PATH / "alphavantage_stock_schemas.json"

    commodities_config_path = CONFIG_PATH / "alphavantage_commodities_config.json"
    commodities_schema_path = CONFIG_PATH / "alphavantage_commodities_schemas.json"

    orchestrator = Orchestrator()
    try:
        # Load directly into dynamic nested paths using label and type variables
        orchestrator.set_config(asset_label="stock", config_type="config", config_data=load_json_file(stock_config_path))
        orchestrator.set_config(asset_label="stock", config_type="schema", config_data=load_json_file(stock_schema_path))
        orchestrator.set_config(asset_label="commodities", config_type="config", config_data=load_json_file(commodities_config_path))
        orchestrator.set_config(asset_label="commodities", config_type="schema", config_data=load_json_file(commodities_schema_path))
    except FileNotFoundError as e:
        print(f"❌ Context Setup Failed: {e.filename} is missing.")
        sys.exit(1)
        
    # Execute everything dynamically 
    orchestrator.execute()

if __name__ == "__main__":
    execute_orchestrator()