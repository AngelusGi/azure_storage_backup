import logging
import os
import sys
import argparse
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
    parser = argparse.ArgumentParser(description="Azure Storage Backup Tool")
    parser.add_argument("--tenant-id", default=os.getenv("ARM_TENANT_ID"))
    parser.add_argument("--client-id", default=os.getenv("ARM_CLIENT_ID"))
    parser.add_argument("--client-secret", default=os.getenv("ARM_CLIENT_SECRET"))
    parser.add_argument(
        "--source-account-blob",
        default=os.getenv("AZURE_SOURCE_STORAGE_ACCOUNT_BLOB"),
    )
    parser.add_argument(
        "--dest-account-blob",
        default=os.getenv("AZURE_DESTINATION_STORAGE_ACCOUNT_BLOB"),
    )
    parser.add_argument(
        "--overwrite-blob",
        default=os.getenv("OVERWRITE_STORAGE_ACCOUNT_BLOB", "False"),
    )
    parser.add_argument(
        "--source-account-queue",
        default=os.getenv("AZURE_SOURCE_STORAGE_ACCOUNT_QUEUE"),
    )
    parser.add_argument(
        "--dest-account-queue",
        default=os.getenv("AZURE_DESTINATION_STORAGE_ACCOUNT_QUEUE"),
    )
    parser.add_argument(
        "--source-account-table",
        default=os.getenv("AZURE_SOURCE_STORAGE_ACCOUNT_TABLE"),
    )
    parser.add_argument(
        "--dest-account-table",
        default=os.getenv("AZURE_DESTINATION_STORAGE_ACCOUNT_TABLE"),
    )
    parser.add_argument(
        "--source-connection-string-file-share",
        default=os.getenv("AZURE_SOURCE_CONNECTION_STRING_FILE_SHARE"),
    )
    parser.add_argument(
        "--dest-connection-string-file-share",
        default=os.getenv("AZURE_DEST_CONNECTION_STRING_FILE_SHARE"),
    )
    args = parser.parse_args()

    setup_logging()

    ### Blob
    blob_replicator = BlobReplicator(
        tenant_id=args.tenant_id,
        client_id=args.client_id,
        client_secret=args.client_secret,
        source_account=args.source_account_blob,
        dest_account=args.dest_account_blob,
        overwrite=args.overwrite_blob.lower() == "true",
    )
    blob_replicator.replicate()

    ### Queue
    queue_replicator = QueueReplicator(
        tenant_id=args.tenant_id,
        client_id=args.client_id,
        client_secret=args.client_secret,
        source_account=args.source_account_queue,
        dest_account=args.dest_account_queue,
    )
    queue_replicator.replicate()

    ### Table
    table_replicator = TableReplicator(
        tenant_id=args.tenant_id,
        client_id=args.client_id,
        client_secret=args.client_secret,
        source_account=args.source_account_table,
        dest_account=args.dest_account_table,
    )
    table_replicator.replicate()

    ## File Share
    file_replicator = FileShareReplicator(
        source_connection_string=args.source_connection_string_file_share,
        dest_connection_string=args.dest_connection_string_file_share,
    )
    file_replicator.replicate()
