from pathlib import Path
import json
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay
import sys
from datetime import datetime
import os

PROJECT_ROOT = Path(__file__).resolve().parents[5]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
SILVER_DATA_DIR = PROJECT_ROOT / "data" / "silver"
CONFIG_DIR = PROJECT_ROOT / "configs"

def pq_to_df(asset_name: str) -> pd.DataFrame:
    # Search recursively under data/raw for the asset's raw files
    parquet_files = sorted(
        RAW_DATA_DIR.rglob(f"*_{asset_name}_raw.pq")
    )

    if not parquet_files:
        print(f"❌ No raw Parquet files located for {asset_name}")
        return pd.DataFrame()

    latest_file_path = parquet_files[-1]

    print(
        f"📖 Reading newest raw file for {asset_name}: "
        f"{latest_file_path}"
    )

    try:
        return pd.read_parquet(latest_file_path)

    except Exception as e:
        print(f"💥 Error reading Parquet file for {asset_name}: {e}")
        return pd.DataFrame()

def flatten_alphavantage_ohlcv(asset_name: str, df_raw: pd.DataFrame, asset_config: dict) -> pd.DataFrame:
    """
    Extracts nested time-series dictionary data from Alpha Vantage raw dataframes,
    flattens the structural schema, and casts numerical columns.
    """
    if df_raw.empty:
        return pd.DataFrame() 
    try:
        # 1. Look up the schema strategy from the asset configuration registry
        schema_type = asset_config.get("schema_type")
        # =====================================================================
        # STRATEGY A: COMMODITIES (XAG, XAU)
        # =====================================================================
        if schema_type == "commodity_gold":
            if "data" in df_raw.columns:
                clean_series = df_raw["data"].dropna()
                df_cleaned = pd.DataFrame(list(clean_series))
            else:
                df_cleaned = df_raw.copy()
            
            # 🟢 FIX: Map 'price' to 'close' so it matches your schema validation rules
            if "price" in df_cleaned.columns:
                df_cleaned = df_cleaned.rename(columns={"price": "close"})

        # =====================================================================
        # STRATEGY B: EQUITIES (NVDA) - Your Original Logic preserved!
        # =====================================================================
        elif schema_type == "equity_daily":
            if "Time Series (Daily)" not in df_raw.columns:
                raise KeyError(f"Expected column 'Time Series (Daily)' was missing for equity {asset_name}")
                
            clean_series = df_raw["Time Series (Daily)"].dropna()
            df_cleaned = pd.DataFrame.from_dict(clean_series.to_dict(), orient="index")
            df_cleaned.columns = [col.split(". ")[-1].strip().replace(" ", "_") for col in df_cleaned.columns]
            df_cleaned.index.name = "date"
            df_cleaned = df_cleaned.reset_index()

        else:
            print(f"⚠️ Skipping {asset_name}: Unknown schema_type configuration structural strategy '{schema_type}'.")
            return pd.DataFrame()
        
        # =====================================================================
        # UNIFIED DATA ENRICHMENT (Runs for BOTH Equities and Commodities)
        # =====================================================================
        # Ensure lineage metadata is tracked cleanly at the front
        if "asset_name" not in df_cleaned.columns:
            df_cleaned.insert(0, "asset_name", asset_name)
            
        # Convert string numbers from the API into proper numeric metrics types
        numeric_cols = ["open", "high", "low", "close", "volume"]
        for col in numeric_cols:
            if col in df_cleaned.columns:
                df_cleaned[col] = pd.to_numeric(df_cleaned[col])
                
        print(f"✨ Transformed {len(df_cleaned)} rows smoothly for {asset_name} using '{schema_type}' engine.")
        return df_cleaned

    except Exception as e:
        print(f"❌ Failed parsing layout structure for {asset_name}: {e}")
        return pd.DataFrame()

def df_column_check(df: pd.DataFrame, expected_schema: dict) -> pd.DataFrame:
    if df.empty or not expected_schema:
        return pd.DataFrame()
    try:
        # 1. Validate that all required columns for THIS specific schema exist
        missing_cols = [col for col in expected_schema.keys() if col not in df.columns]
        if missing_cols:
            print(f"❌ SCHEMA VALIDATION FAILED: Missing columns {missing_cols}")
            return pd.DataFrame()
    # 2. Slice out extra columns and enforce strict schema order
        df_validated = df[list(expected_schema.keys())].copy()
        
        # 3. Enforce Datatypes dynamically based on the passed schema rules
        if "date" in df_validated.columns and not pd.api.types.is_datetime64_any_dtype(df_validated["date"]):
            df_validated["date"] = pd.to_datetime(df_validated["date"])

        for col, dtype in expected_schema.items():
            if col != "date":
                df_validated[col] = df_validated[col].astype(dtype)

        return df_validated
    except Exception as e:
        print(f"❌ Critical error during schema enforcement: {e}")
        return pd.DataFrame()

def derive_time_attributes(df: pd.DataFrame, date_column: str = "date") -> pd.DataFrame:
    """
    Enriches a DataFrame by parsing a date string column into granular 
    time attributes (year, month, day, day_of_week) for optimized analytics.
    """
    if df.empty or date_column not in df.columns:
        return df
    
    df_enriched = df.copy()
    
    # 1. Ensure the column is explicitly cast to a Pandas datetime object
    if not pd.api.types.is_datetime64_any_dtype(df_enriched[date_column]):
        df_enriched[date_column] = pd.to_datetime(df_enriched[date_column])

    # 2. Extract and insert the granular time coordinates directly after the date column
    # Using the .dt accessor computes these attributes instantly for the entire column
    df_enriched.insert(1, "year", df_enriched[date_column].dt.year)
    df_enriched.insert(2, "month", df_enriched[date_column].dt.month)
    df_enriched.insert(3, "day", df_enriched[date_column].dt.day)
    df_enriched.insert(4, "day_of_week", df_enriched[date_column].dt.dayofweek)  # 0=Monday, 6=Sunday

    return df_enriched

def calculate_percentage_changes(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "close" not in df.columns or "date" not in df.columns:
        return df

    # 1. Sort chronologically by timeline
    df_calc = df.sort_values(by=["asset_name", "date"]).reset_index(drop=True)

    # 2. Grab the prior row's details
    df_calc["prev_close"] = df_calc.groupby("asset_name")["close"].shift(1)
    df_calc["prev_date"] = df_calc.groupby("asset_name")["date"].shift(1)

    # 3. Calculate percent change normally
    df_calc["pct_change"] = (df_calc["close"] - df_calc["prev_close"]) / df_calc["prev_close"]

    # 4. 🔥 THE HOLIDAY-AWARE GUARD
    # Load the official US holiday calendar profile
    us_bd = CustomBusinessDay(calendar=USFederalHolidayCalendar())

    # We loop through rows where a previous date exists to calculate the actual trading day gap
    def verify_trading_gap(row):
        if pd.isna(row["prev_date"]) or pd.isna(row["date"]):
            return 1 # Baseline safe state
            
        # Calculate how many business days SHOULD be between these two points
        # Using us_bd strips out standard weekends AND federal holidays automatically!
        expected_business_days = len(pd.date_range(start=row["prev_date"], end=row["date"], freq=us_bd)) - 1
        return expected_business_days

    # 5. Apply the trading day calculation gap check
    df_calc["trading_day_gap"] = df_calc.apply(verify_trading_gap, axis=1)

    # If the gap is > 1 trading day, it means a real market session was lost or skipped
    is_data_missing = df_calc["trading_day_gap"] > 1
    df_calc.loc[is_data_missing, "pct_change"] = None

    # Audit logging
    gap_count = is_data_missing.sum()
    if gap_count > 0:
        print(f"⚠️ DATA GAP FIREWALL: Set 'pct_change' to NaN on {gap_count} rows due to dropped/missing trading sessions.")

    # Clean up tracking features
    df_calc = df_calc.drop(columns=["prev_close", "prev_date", "trading_day_gap"])
    return df_calc

def df_integrity_check(df: pd.DataFrame) -> pd.DataFrame:
    """
    Data Integrity Firewall: Validates financial logic across columns.
    Filters out rows that violate market rules.
    """
    if df.empty:
        return df
    df_cleaned = df.copy()
    # 1. Base rule: Prices must be greater than zero (Runs for ALL assets)
    if "close" in df_cleaned.columns:
        df_cleaned = df_cleaned[df_cleaned["close"] > 0]

    # 2. 🟢 DYNAMIC EQUITY RULES (Only runs if 'high', 'low', 'open' exist)
    if all(col in df_cleaned.columns for col in ["high", "low", "open"]):
        # High must be greater than or equal to Open and Close
        df_cleaned = df_cleaned[df_cleaned["high"] >= df_cleaned["open"]]
        df_cleaned = df_cleaned[df_cleaned["high"] >= df_cleaned["close"]]
        
        # Low must be less than or equal to Open and Close
        df_cleaned = df_cleaned[df_cleaned["low"] <= df_cleaned["open"]]
        df_cleaned = df_cleaned[df_cleaned["low"] <= df_cleaned["close"]]
        
        # Prices must be positive numbers
        df_cleaned = df_cleaned[df_cleaned["open"] > 0]
        df_cleaned = df_cleaned[df_cleaned["high"] > 0]
        df_cleaned = df_cleaned[df_cleaned["low"] > 0]

    return df_cleaned

def save_silver_to_pq(asset_name: str, df: pd.DataFrame):
    if df.empty:
        raise ValueError(
            f"Cannot save Silver data for {asset_name}: DataFrame is empty."
        )

    silver_dir = SILVER_DATA_DIR / asset_name
    silver_dir.mkdir(parents=True, exist_ok=True)

    file_path = silver_dir / f"{asset_name}_silver.pq"

    df.to_parquet(file_path, engine="pyarrow", index=False)

    print(f"💾 Saved silver Parquet file to: {file_path}")

def run_alphavantage_commodities_silver(configs: dict, asset_schemas: dict) -> bool:

    for asset_name, asset_config in configs.items():
        success = True

        df_raw = pq_to_df(asset_name)
        if df_raw.empty:
            print(f"⚠️ Skipping {asset_name}: No raw data found.")
            success = False
            continue

        # Step 2: Unpack nested dictionaries and generate dynamic dates
        df_flattened = flatten_alphavantage_ohlcv(asset_name, df_raw, asset_config)
        # Step 3: Run the Schema Firewall dynamically
        # 🟢 1. Finds "equity_daily" or "commodity_gold"
        schema_key = asset_config.get("schema_type")
        # 🟢 2. Grabs the expected data type dictionary mapping
        target_schema = asset_schemas.get(schema_key) 

        if not target_schema:
            print(f"⚠️ Skipping {asset_name}: Schema type '{schema_key}' not found.")
            success = False
            continue

        # 🟢 3. Passes the dictionary schema map to your column checker
        df_schema_approved = df_column_check(df_flattened, expected_schema=target_schema)
        if df_schema_approved.empty:
            print(f"⚠️ Skipping {asset_name}: Failed Schema Firewall validation.")
            success = False
            continue
        # Step 4: Run the Time Attribute Enrichment Function
        df_enriched = derive_time_attributes(df_schema_approved, date_column="date")

        # Step 5: FINANCIAL METRICS (Safely calculate chronological market changes)
        # Ensure it's sorted oldest-to-newest before calculating percentage changes!
        df_enriched = df_enriched.sort_values(by="date", ascending=True)
        df_with_metrics = calculate_percentage_changes(df_enriched)

        # Step 6: LOGICAL INTEGRITY FIREWALL (Check high >= open, prices > 0)
        df_clean = df_integrity_check(df_with_metrics)
        if df_clean.empty:
            print(f"⚠️ Skipping {asset_name}: Integrity check returned empty data.")
            success = False
            continue
        save_silver_to_pq(asset_name, df_clean)
        print(f"✅ Silver transformation successful for {asset_name} using '{schema_key}' schema.\n")
    return success
def load_json_file(file_path):
    """Safely loads a JSON file or halts execution if missing."""
    try:
        with open(file_path, "r") as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"❌ Critical Failure: File not found at '{file_path}'")
        sys.exit(1) # Stop the script immediately instead of crashing later

if __name__ == "__main__":
    # 1. Resolve paths cleanly
    config_path = PROJECT_ROOT / "config.json"
    asset_schema_path = PROJECT_ROOT / "alphavantage_asset_schemas.json"

    # 2. Load configurations using the helper
    config_data = load_json_file(config_path)
    asset_schema = load_json_file(asset_schema_path)

    # 3. Trigger the pipeline
    run_alphavantage_commodities_silver(config_data, asset_schema)
