import os
import argparse
from modules.stg_blob import BlobReplicator
from modules.stg_file import FileShareReplicator
from modules.stg_queue import QueueReplicator
from modules.stg_table import TableReplicator
from modules.stg_logger import setup_logging

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

    setup_logging("backup_tool.log")

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
