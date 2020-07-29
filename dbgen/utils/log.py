# External imports
import logging
import sys
from logging import Logger
import logging.config
from logging.handlers import RotatingFileHandler
from os.path import exists, join, dirname
from pathlib import Path
import yaml

# Internal Imports
from .config import config as dbgen_config

# default_log_yaml = Path(__file__).parent / "default_logging.yaml"

# def setup_logging(default_path=default_log_yaml, default_level=logging.INFO):
#     """Setup logging configuration"""
#     if default_path.exists():
#         config = yaml.safe_load(default_log_yaml.read_text())
#         breakpoint()
#         logging.config.dictConfig(config)
#     else:
#         logging.basicConfig(level=default_level)
#     logging.config.dictConfig(dbgen_config)

default_log_path = Path().home() / ".dbgen/dbgen.log"
default_log_path = Path(
    dbgen_config.get("dbgen", "log_path", fallback=default_log_path)
)


def setup_logger(
    logger_name: str = "",
    level: int = logging.INFO,
    write_logs: bool = False,
    log_path: Path = default_log_path,
) -> Logger:
    format = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
    custom_logger = logging.getLogger(logger_name)
    custom_logger.setLevel(level)
    custom_logger.propagate = False
    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setFormatter(format)
    console_handler.setLevel(logging.WARNING)
    custom_logger.addHandler(console_handler)
    if write_logs:
        log_path.parent.mkdir(exist_ok=True, parents=True)
        info_handler = RotatingFileHandler(log_path, maxBytes=10485760, backupCount=1)
        info_handler.setLevel(level)
        info_handler.setFormatter(format)
        custom_logger.addHandler(info_handler)

    return custom_logger
