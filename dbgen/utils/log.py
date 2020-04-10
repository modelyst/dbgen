# External imports
import logging
import sys
from logging import Logger
from logging.handlers import RotatingFileHandler

# Internal Imports


def setup_logger(
    logger_name: str = "", level: int = logging.INFO, write_logs: bool = False
) -> Logger:
    format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    custom_logger = logging.getLogger(logger_name)
    custom_logger.propagate = False
    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setFormatter(format)
    console_handler.setLevel(logging.WARNING)
    custom_logger.addHandler(console_handler)
    if write_logs:
        info_handler = RotatingFileHandler("./dbgen.log", maxBytes=10000, backupCount=1)
        info_handler.setLevel(level)
        info_handler.setFormatter(format)
        custom_logger.addHandler(info_handler)

    return custom_logger
