from pathlib import Path

# Project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Configuration directories
CONFIG_PATH = PROJECT_ROOT / "configs"

# Env paths
ENV_PATH = PROJECT_ROOT / "alphavantage_api.env"

# Data directories
DATA_PATH = PROJECT_ROOT / "data"
RAW_PATH = DATA_PATH / "raw"
SILVER_PATH = DATA_PATH / "silver"
GOLD_PATH = DATA_PATH / "gold"

# Asset-specific directories
COMMODITIES_PATH = RAW_PATH / "commodities"
STOCKS_PATH = RAW_PATH / "stocks"