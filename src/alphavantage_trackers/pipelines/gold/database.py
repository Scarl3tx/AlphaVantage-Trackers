import duckdb

from alphavantage_trackers.paths import DATABASE_PATH

def get_database_connection():

    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)

    return duckdb.connect(str(DATABASE_PATH))