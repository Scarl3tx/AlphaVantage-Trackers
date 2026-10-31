import logging
from pathlib import Path

from alphavantage_trackers.paths import PROJECT_ROOT

LOG_PATH = PROJECT_ROOT / "logs"


def get_pipeline_logger(pipeline_name: str):

    log_dir = LOG_PATH / pipeline_name
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(pipeline_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not logger.handlers:

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%H:%M:%S"
        )

        file_handler = logging.FileHandler(
            log_dir / f"{pipeline_name}.log",
            encoding="utf-8"
        )

        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger