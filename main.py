import os
import argparse
import concurrent.futures
import logging
from modules.stg_blob import BlobReplicator
from modules.stg_file import FileShareReplicator
from modules.stg_queue import QueueReplicator
from modules.stg_table import TableReplicator
from modules.stg_logger import setup_logging


def run_blob_replication(args):
    """Run blob replication in a separate thread"""
    if not all([args.source_account_blob, args.dest_account_blob]):
        logging.info("Skipping blob replication - missing configuration")
        return

    blob_replicator = BlobReplicator(
        tenant_id=args.tenant_id,
        client_id=args.client_id,
        client_secret=args.client_secret,
        source_account=args.source_account_blob,
        dest_account=args.dest_account_blob,
        overwrite=args.overwrite_blob == "true",
        max_retries=args.retry_count,
        retry_delay=args.retry_delay_in_seconds,
    )
    blob_replicator.replicate()


def run_queue_replication(args):
    """Run queue replication in a separate thread"""
    if not all([args.source_account_queue, args.dest_account_queue]):
        logging.info("Skipping queue replication - missing configuration")
        return

    queue_replicator = QueueReplicator(
        tenant_id=args.tenant_id,
        client_id=args.client_id,
        client_secret=args.client_secret,
        source_account=args.source_account_queue,
        dest_account=args.dest_account_queue,
        max_retries=args.retry_count,
        retry_delay=args.retry_delay_in_seconds,
    )
    queue_replicator.replicate()


def run_table_replication(args):
    """Run table replication in a separate thread"""
    if not all([args.source_account_table, args.dest_account_table]):
        logging.info("Skipping table replication - missing configuration")
        return

    table_replicator = TableReplicator(
        tenant_id=args.tenant_id,
        client_id=args.client_id,
        client_secret=args.client_secret,
        source_account=args.source_account_table,
        dest_account=args.dest_account_table,
        max_retries=args.retry_count,
        retry_delay=args.retry_delay_in_seconds,
    )
    table_replicator.replicate()


def run_file_replication(args):
    """Run file share replication in a separate thread"""
    if not all(
        [
            args.source_connection_string_file_share,
            args.dest_connection_string_file_share,
        ]
    ):
        logging.info("Skipping file share replication - missing configuration")
        return

    file_replicator = FileShareReplicator(
        source_connection_string=args.source_connection_string_file_share,
        dest_connection_string=args.dest_connection_string_file_share,
    )
    file_replicator.replicate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Azure Storage Backup Tool")
    parser.add_argument(
        "--tenant-id",
        default=os.getenv("ARM_TENANT_ID"),
        type=str,
    )
    parser.add_argument(
        "--client-id",
        default=os.getenv("ARM_CLIENT_ID"),
        type=str,
    )
    parser.add_argument(
        "--client-secret",
        default=os.getenv("ARM_CLIENT_SECRET"),
        type=str,
    )
    parser.add_argument(
        "--source-account-blob",
        default=os.getenv("AZURE_SOURCE_STORAGE_ACCOUNT_BLOB"),
        type=str,
    )
    parser.add_argument(
        "--dest-account-blob",
        default=os.getenv("AZURE_DESTINATION_STORAGE_ACCOUNT_BLOB"),
        type=str,
    )
    parser.add_argument(
        "--overwrite-blob",
        default=os.getenv("OVERWRITE_STORAGE_ACCOUNT_BLOB", "false"),
        type=bool,
    )
    parser.add_argument(
        "--source-account-queue",
        default=os.getenv("AZURE_SOURCE_STORAGE_ACCOUNT_QUEUE"),
        type=str,
    )
    parser.add_argument(
        "--dest-account-queue",
        default=os.getenv("AZURE_DESTINATION_STORAGE_ACCOUNT_QUEUE"),
        type=str,
    )
    parser.add_argument(
        "--source-account-table",
        default=os.getenv("AZURE_SOURCE_STORAGE_ACCOUNT_TABLE"),
        type=str,
    )
    parser.add_argument(
        "--dest-account-table",
        default=os.getenv("AZURE_DESTINATION_STORAGE_ACCOUNT_TABLE"),
        type=str,
    )
    parser.add_argument(
        "--source-connection-string-file-share",
        default=os.getenv("AZURE_SOURCE_CONNECTION_STRING_FILE_SHARE"),
        type=str,
    )
    parser.add_argument(
        "--dest-connection-string-file-share",
        default=os.getenv("AZURE_DEST_CONNECTION_STRING_FILE_SHARE"),
        type=str,
    )
    parser.add_argument(
        "--retry-delay-in-seconds",
        default=os.getenv("RETRY_DELAY_IN_SECONDS", 10),
        type=int,
    )
    parser.add_argument(
        "--retry-count",
        default=os.getenv("RETRY_COUNT", 3),
        type=int,
    )

    args = parser.parse_args()

    setup_logging("backup_tool.log")

    logging.info("Starting Azure Storage Backup Tool with parallel processing")

    # Run all storage replications in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = []

        # Submit replication tasks
        futures.append(executor.submit(run_blob_replication, args))
        futures.append(executor.submit(run_queue_replication, args))
        futures.append(executor.submit(run_table_replication, args))
        futures.append(executor.submit(run_file_replication, args))

        # Wait for all replications to complete
        completed_futures = concurrent.futures.as_completed(futures)
        for future in completed_futures:
            try:
                future.result()  # This will raise any exceptions that occurred
            except Exception as e:
                logging.error(f"Replication task failed: {e}")

    logging.info("All replication tasks completed")
