import asyncio

from alphavantage_trackers.pipelines.av_commodities.av_commodities_pipeline import (
    run_commodities_pipeline
)
from alphavantage_trackers.pipelines.av_stocks.av_stocks_pipeline import (
    run_stocks_pipeline
)
from alphavantage_trackers.pipelines.gold.gold_pipeline import (
    run_gold_pipeline
)

from alphavantage_trackers.logging_config import get_pipeline_logger
logger = get_pipeline_logger("main")

async def main():

    print("\n========== ALPHAVANTAGE TRACKER ==========")
    logger.info("Main Pipeline Started")
    pipelines = {
        "COMMODITIES": run_commodities_pipeline,
        "STOCKS": run_stocks_pipeline
    }

    # Run Bronze/Silver pipelines concurrently
    tasks = {
        name: asyncio.to_thread(pipeline)
        for name, pipeline in pipelines.items()
    }

    results = await asyncio.gather(
        *tasks.values(),
        return_exceptions=True
    )

    # Collect pipeline results
    overall_success = True

    for name, result in zip(tasks.keys(), results):

        if isinstance(result, Exception):
            logger.exception(
                "[%s] Pipeline encountered an error.",
                name,
                exc_info=result
            )
            overall_success = False

        elif result is False:
            logger.error("[%s] Pipeline failed.", name)
            overall_success = False

        else:
            logger.info("[%s] Pipeline completed successfully.", name)

    if overall_success:
        logger.info("All Bronze/Silver pipelines succeeded. Starting Gold.")
        gold_success = await asyncio.to_thread(run_gold_pipeline)
        if gold_success:
            logger.info("Gold pipeline completed successfully.")
        else:
            logger.error("Gold pipeline failed.")
            overall_success = False

    else:
        logger.warning(
            "At least one Bronze/Silver pipeline failed. Skipping Gold."
        )

    if overall_success:
        print("\n========== ALL PIPELINES COMPLETED ==========")
        logger.info("\n========== ALL PIPELINES COMPLETED ==========")
    else:
        print("\n========== SOME PIPELINES FAILED ==========")
        logger.warning("\n========== SOME PIPELINES FAILED ==========")
if __name__ == "__main__":
    asyncio.run(main())