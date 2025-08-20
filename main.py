import logging
import os
import sys
from stg_blob import BlobReplicator
from stg_file import FileShareReplicator
from stg_queue import QueueReplicator
from stg_table import TableReplicator


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
    logging.info(f"Default logger initialized as {log_level}")
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
        logging.info(f"Logger {logger_name} initialized as {azure_log_level}")


if __name__ == "__main__":
    setup_logging()

    ### Blob
    blob_replicator = BlobReplicator(
        tenant_id=os.getenv("ARM_TENANT_ID"),
        client_id=os.getenv("ARM_CLIENT_ID"),
        client_secret=os.getenv("ARM_CLIENT_SECRET"),
        source_account=os.getenv("AZURE_SOURCE_STORAGE_ACCOUNT_BLOB"),
        dest_account=os.getenv("AZURE_DESTINATION_STORAGE_ACCOUNT_BLOB"),
        overwrite=(
            os.getenv("OVERWRITE_STORAGE_ACCOUNT_BLOB", "False").lower() == "true"
        ),
    )
    blob_replicator.replicate()

    ### Queue
    queue_replicator = QueueReplicator(
        tenant_id=os.getenv("ARM_TENANT_ID"),
        client_id=os.getenv("ARM_CLIENT_ID"),
        client_secret=os.getenv("ARM_CLIENT_SECRET"),
        source_account=os.getenv("AZURE_SOURCE_STORAGE_ACCOUNT_QUEUE"),
        dest_account=os.getenv("AZURE_DESTINATION_STORAGE_ACCOUNT_QUEUE"),
    )
    queue_replicator.replicate()

    ### Table
    table_replicator = TableReplicator(
        tenant_id=os.getenv("ARM_TENANT_ID"),
        client_id=os.getenv("ARM_CLIENT_ID"),
        client_secret=os.getenv("ARM_CLIENT_SECRET"),
        source_account=os.getenv("AZURE_SOURCE_STORAGE_ACCOUNT_TABLE"),
        dest_account=os.getenv("AZURE_DESTINATION_STORAGE_ACCOUNT_TABLE"),
    )
    table_replicator.replicate()

    ## File Share
    file_replicator = FileShareReplicator(
        source_connection_string=os.getenv("AZURE_SOURCE_CONNECTION_STRING_FILE_SHARE"),
        dest_connection_string=os.getenv("AZURE_DEST_CONNECTION_STRING_FILE_SHARE"),
    )
    file_replicator.replicate()
