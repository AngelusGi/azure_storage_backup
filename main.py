import os
from stg_blob import BlobReplicator, setup_logging as setup_blob_logging
from stg_file import FileShareReplicator, setup_logging as setup_file_logging
from stg_queue import QueueReplicator, setup_logging as setup_queue_logging
from stg_table import TableReplicator, setup_logging as setup_table_logging

if __name__ == "__main__":
    ### Blob
    setup_blob_logging()
    blob_replicator = BlobReplicator(
        tenant_id=os.getenv("AZURE_TENANT_ID"),
        client_id=os.getenv("AZURE_CLIENT_ID"),
        client_secret=os.getenv("AZURE_CLIENT_SECRET"),
        source_account=os.getenv("AZURE_SOURCE_STORAGE_ACCOUNT_BLOB"),
        dest_account=os.getenv("AZURE_DESTINATION_STORAGE_ACCOUNT_BLOB"),
        overwrite=(
            os.getenv("OVERWRITE_STORAGE_ACCOUNT_BLOB", "False").lower() == "true"
        )
    )
    blob_replicator.replicate()

    ### Queue
    setup_queue_logging()
    queue_replicator = QueueReplicator(
        tenant_id=os.getenv("AZURE_TENANT_ID"),
        client_id=os.getenv("AZURE_CLIENT_ID"),
        client_secret=os.getenv("AZURE_CLIENT_SECRET"),
        source_account=os.getenv("AZURE_SOURCE_STORAGE_ACCOUNT_QUEUE"),
        dest_account=os.getenv("AZURE_DESTINATION_STORAGE_ACCOUNT_QUEUE"),
    )
    queue_replicator.replicate()

    ### Table
    setup_table_logging()
    table_replicator = TableReplicator(
        tenant_id=os.getenv("AZURE_TENANT_ID"),
        client_id=os.getenv("AZURE_CLIENT_ID"),
        client_secret=os.getenv("AZURE_CLIENT_SECRET"),
        source_account=os.getenv("AZURE_SOURCE_STORAGE_ACCOUNT_TABLE"),
        dest_account=os.getenv("AZURE_DESTINATION_STORAGE_ACCOUNT_TABLE"),
    )
    table_replicator.replicate()

    ## File Share
    setup_file_logging()
    file_replicator = FileShareReplicator(
        source_connection_string=os.getenv("AZURE_SOURCE_CONNECTION_STRING_FILE_SHARE"),
        dest_connection_string=os.getenv("AZURE_DEST_CONNECTION_STRING_FILE_SHARE"),
    )
    file_replicator.replicate()
