import asyncio

from alphavantage_trackers.pipelines.av_commodities.av_commodities_pipeline import (
    run_commodities_pipeline
)

from alphavantage_trackers.pipelines.av_stocks.av_stocks_pipeline import (
    run_stocks_pipeline
)


async def main():

    print("\n========== ALPHAVANTAGE TRACKER ==========")

    pipelines = {
        "COMMODITIES": run_commodities_pipeline,
        "STOCKS": run_stocks_pipeline
    }

    # Run pipelines concurrently
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
            print(f"[{name}] Pipeline encountered an error: {result}")
            overall_success = False

        elif result is False:
            print(f"[{name}] Pipeline failed.")
            overall_success = False

        else:
            print(f"[{name}] Pipeline completed successfully.")

    if overall_success:
        print("\n========== ALL PIPELINES COMPLETED ==========")
    else:
        print("\n========== SOME PIPELINES FAILED ==========")


if __name__ == "__main__":
    asyncio.run(main())