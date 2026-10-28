
import logging

from src.bronze.extract_alphavantage_stocks_data import (
    run_alphavantage_stock_bronze
)
from src.bronze.extract_alphavantage_commodities_data import (
    run_alphavantage_commodities_bronze
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Registry of available Bronze pipelines
BRONZE_REGISTRY = {
    "stock": {
        "runner": run_alphavantage_stock_bronze,
        "config_key": "stocks",
    },
    "commodities": {
        "runner": run_alphavantage_commodities_bronze,
        "config_key": "commodities",
    },
}


def run_bronze_extraction(
    pipelines_to_run: list[dict],
) -> dict[str, bool]:

    status_report = {}

    for pipeline in pipelines_to_run:
        label = pipeline["label"]
        config = pipeline["config"]

        # Find the pipeline in the registry
        pipeline_info = BRONZE_REGISTRY.get(label)

        if pipeline_info is None:
            logging.error(
                "No Bronze function registered for '%s'.",
                label,
            )
            status_report[label] = False
            continue

        try:
            print()
            print()
            logging.info("Starting %s extraction...", label)

            # Retrieve the runner and its corresponding config key
            runner = pipeline_info["runner"]
            config_key = pipeline_info["config_key"]

            # Extract the asset-specific configuration
            asset_configs = config.get(config_key, {})

            if not asset_configs:
                logging.error(
                    "No asset configurations found for '%s' "
                    "(config key: '%s').",
                    label,
                    config_key,
                )
                status_report[label] = False
                continue

            # Execute the pipeline
            success = runner(configs=asset_configs)

            # Record the result
            status_report[label] = success is True

            if success is True:
                logging.info(
                    "Finished %s extraction successfully.",
                    label,
                )
            else:
                logging.error(
                    "%s extraction completed with failures.",
                    label,
                )

        except Exception:
            logging.exception(
                "Extraction failed for '%s'.",
                label,
            )
            status_report[label] = False

    return status_report