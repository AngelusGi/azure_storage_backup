import logging
import os
import sys


def setup_logging(log_file: str = "backup_tool.log") -> None:
    log_level_str = os.getenv("REPLICA_LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, mode="w"),
        ],
    )
    logging.info(f"Default logger initialized as {log_level_str}")
    azure_log_level_str = os.getenv("AZURE_LOG_LEVEL", "WARNING").upper()
    azure_log_level = getattr(logging, azure_log_level_str, logging.WARNING)
    azure_loggers = [
        "azure",
        "azure.core.pipeline",
        "azure.identity",
        "azure.storage.blob",
    ]
    for logger_name in azure_loggers:
        logger = logging.getLogger(logger_name)
        logger.setLevel(azure_log_level)
        logging.info(f"Logger {logger_name} initialized as {azure_log_level_str}")
