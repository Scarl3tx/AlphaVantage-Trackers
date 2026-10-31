import logging
import re

from alphavantage_trackers.paths import STOCKS_SILVER_PATH

logger = logging.getLogger("stocks")

def load_stocks_to_gold(conn):
    parquet_files = list(
        STOCKS_SILVER_PATH.glob("*/*_silver.pq")
    )

    if not parquet_files:
        logger.info("No stocks Silver Parquet files found.")
        return True

    conn.execute("CREATE SCHEMA IF NOT EXISTS stocks")

    success = True

    for parquet_file in parquet_files:
        asset_name = parquet_file.parent.name

        # Validate table name before using it in SQL
        if not re.fullmatch(r"[A-Za-z0-9_]+", asset_name):
            logger.error("Invalid stocks table name: %s", asset_name)
            success = False
            continue

        table_name = f'"{asset_name}"'
        parquet_path = str(parquet_file)

        try:
            # Get the columns in the incoming Silver dataset
            source_columns = [
                column[0]
                for column in conn.execute(
                    "SELECT * FROM read_parquet(?) LIMIT 0",
                    [parquet_path]
                ).description
            ]

            if "date" not in source_columns:
                raise ValueError(
                    f"Missing required merge key 'date' for {asset_name}"
                )

            # Check whether the Gold table already exists
            table_exists = conn.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = 'stocks'
                  AND table_name = ?
                """,
                [asset_name]
            ).fetchone()[0] > 0

            # First load: create the table
            if not table_exists:
                conn.execute(
                    f"""
                    CREATE TABLE stocks.{table_name} AS
                    SELECT *
                    FROM read_parquet(?)
                    """,
                    [parquet_path]
                )

                logger.info(
                    "Created Gold table for stocks %s.",
                    asset_name
                )
                continue

            # Get existing Gold table columns
            target_columns = [
                row[0]
                for row in conn.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'stocks'
                      AND table_name = ?
                    ORDER BY ordinal_position
                    """,
                    [asset_name]
                ).fetchall()
            ]

            if set(source_columns) != set(target_columns):
                raise ValueError(
                    f"Column mismatch for {asset_name}. "
                    f"Silver: {source_columns}; Gold: {target_columns}"
                )

            # Build the UPDATE and INSERT clauses dynamically
            def quote_identifier(column):
                return '"' + column.replace('"', '""') + '"'

            update_columns = [
                column for column in source_columns
                if column != "date"
            ]

            update_clause = ", ".join(
                f"{quote_identifier(column)} = "
                f"source.{quote_identifier(column)}"
                for column in update_columns
            )

            insert_columns = ", ".join(
                quote_identifier(column)
                for column in source_columns
            )

            insert_values = ", ".join(
                f"source.{quote_identifier(column)}"
                for column in source_columns
            )

            # Upsert into Gold
            merge_sql = f"""
                MERGE INTO stocks.{table_name} AS target
                USING read_parquet(?) AS source
                ON target."date" = source."date"
                WHEN MATCHED THEN
                    UPDATE SET {update_clause}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_columns})
                    VALUES ({insert_values})
            """

            conn.execute(merge_sql, [parquet_path])

            logger.info(
                "Upserted stocks %s into DuckDB.",
                asset_name
            )

        except Exception:
            logger.exception(
                "Failed to load stocks %s into DuckDB.",
                asset_name
            )
            success = False

    return success