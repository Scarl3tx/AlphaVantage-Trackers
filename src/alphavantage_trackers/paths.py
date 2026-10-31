from pathlib import Path

# Project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Configuration directories
CONFIG_PATH = PROJECT_ROOT / "configs"

# Env paths
ENV_PATH = PROJECT_ROOT / "alphavantage_api.env"

# Data directories
DATA_PATH = PROJECT_ROOT / "data"
DATABASE_PATH = DATA_PATH / "database" / "alphavantage.duckdb"

RAW_PATH = DATA_PATH / "raw"

SILVER_PATH = DATA_PATH / "silver"
STOCKS_SILVER_PATH = SILVER_PATH / "stocks"
COMMODITIES_SILVER_PATH = SILVER_PATH / "commodities"

GOLD_PATH = DATA_PATH / "gold"

#API Limit tracker
API_TRACKER_PATH = DATA_PATH / "api_limit_tracker"

# Asset-specific directories
COMMODITIES_PATH = RAW_PATH / "commodities"
STOCKS_PATH = RAW_PATH / "stocks"