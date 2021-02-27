"""Configure the dbgen logger for each run"""
# External imports
import logging
import sys
from logging import Logger
import logging.config
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logger(
    logger_name: str,
    level: int,
    write_logs: bool,
    log_path: Path,
    log_format: str,
    log_to_stdout: bool = False,
) -> Logger:
    """
    configures the dbgen logger for a given model

    Args:
        logger_name (str, optional): name of the logger. Defaults to "".
        level (int, optional): level of the logger. Defaults to logging.INFO.
        write_logs (bool, optional): should the log be written to a file. Defaults to False.
        log_path (Path, optional): if write_logs write logs to this path.
        Defaults to default_log_path.

    Returns:
        Logger: the Logger object
    """
    format = logging.Formatter(log_format)
    custom_logger = logging.getLogger(logger_name)
    custom_logger.setLevel(level)
    custom_logger.propagate = False
    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setFormatter(format)
    # Only log to screen if flag is set
    console_level = level if log_to_stdout else logging.WARNING
    console_handler.setLevel(console_level)
    custom_logger.addHandler(console_handler)
    if write_logs:
        log_path.parent.mkdir(exist_ok=True, parents=True)
        info_handler = RotatingFileHandler(log_path, maxBytes=10485760, backupCount=1)
        info_handler.setLevel(level)
        info_handler.setFormatter(format)
        custom_logger.addHandler(info_handler)

    return custom_logger
