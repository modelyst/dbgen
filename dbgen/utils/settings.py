"""Initialize dbgen"""
import logging
from typing import Optional
from pathlib import Path
from ..core.misc import ConnectInfo
from .config import config
from .log import setup_logger

logger = logging.getLogger("dbgen.settings")
# Initalize variables
DB_CONN_STR: Optional[str] = None
METADB_CONN_STR: Optional[str] = None
DB_SCHEMA: Optional[str] = None
METADB_SCHEMA: Optional[str] = None
CONN: Optional[ConnectInfo] = None
META_CONN: Optional[ConnectInfo] = None


def configure_vars():
    global DB_CONN_STR
    global METADB_CONN_STR
    global DB_SCHEMA
    global METADB_SCHEMA
    DB_CONN_STR = config.get("core", "DB_CONN_STR")
    METADB_CONN_STR = config.get("core", "METADB_CONN_STR", fallback=None)
    DB_SCHEMA = config.get("core", "DB_SCHEMA", fallback=None)
    METADB_SCHEMA = config.get("core", "METADB_SCHEMA", fallback=None)


def configure_connections():
    """
    Cycles through database connection variables until first on set is found.

    Order is db_connection_str
    !TODO! Implement airflow_conn_id 
    """
    global CONN
    global META_CONN
    # Main Database
    if DB_CONN_STR:
        CONN = ConnectInfo.from_dsn(DB_CONN_STR, DB_SCHEMA)
    else:
        raise ValueError(
            "No Connection string at any of the following locations:"
            + "\n".join(["DBGEN__CORE__DB_CONN_STR", "config(core.DB_CONN_STR)"])
        )

    if METADB_CONN_STR:
        META_CONN = ConnectInfo.from_dsn(METADB_CONN_STR, METADB_SCHEMA)
    else:
        logger.warning(
            f"Did not find a envvar or config value for meta_db conn using CONN_STR with schema {CONN.schema}_log"
        )
        META_CONN = CONN.copy()
        if METADB_SCHEMA:
            META_CONN.schema = METADB_SCHEMA
        else:
            META_CONN.schema = META_CONN.schema + "_log"
        assert META_CONN != CONN


def configure_logging():
    log_path = config.get("logging", "log_path")
    log_level = config.get("logging", "log_level")
    write_logs = config.getboolean("logging", "write_logs")
    log_format = config.get("logging", "log_format")
    log_to_stdout = config.getboolean("logging", "log_to_stdout")
    setup_logger(
        "dbgen",
        log_level,
        write_logs=write_logs,
        log_path=Path(log_path),
        log_format=log_format,
        log_to_stdout=log_to_stdout,
    )


def initialize():
    configure_logging()
    configure_vars()
    configure_connections()
