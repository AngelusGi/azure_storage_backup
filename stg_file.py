import subprocess
import os
import logging
import sys
from azure.storage.fileshare import (
    ShareProperties,
)  # azure-storage-file-share

connection_string = os.getenv("STORAGE_CONNECTION_STRING")


def run_azcopy_sync(source_sas_url: str, dest_sas_url: str):
    """Execute azcopy sync command using subprocess"""
    try:
        logging.info(f"Starting azcopy sync from {source_sas_url} to {dest_sas_url}")

        # Build the azcopy command
        cmd = ["azcopy", "sync", source_sas_url, dest_sas_url]

        # Execute the command
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)

        logging.info(f"azcopy sync completed successfully")
        logging.info(f"stdout: {result.stdout}")

        return True

    except subprocess.CalledProcessError as e:
        logging.error(f"azcopy sync failed with return code {e.returncode}")
        logging.error(f"stderr: {e.stderr}")
        logging.error(f"stdout: {e.stdout}")
        return False
    except FileNotFoundError:
        logging.error(
            "azcopy command not found. Please ensure azcopy is installed and in PATH"
        )
        return False
    except Exception as e:
        logging.error(f"Unexpected error during azcopy sync: {str(e)}")
        return False


def create_share_with_quota_and_metadata(
    connection_string_destination_storage: str, share_to_clone: ShareProperties
):
    # [START create_share_client_from_conn_string]
    from azure.storage.fileshare import ShareClient

    share_client = ShareClient.from_connection_string(
        connection_string_destination_storage, share_to_clone.name
    )
    # [END create_share_client_from_conn_string]

    # Create the share
    share_client.create_share(
        metadata=share_to_clone.metadata,
        access_tier=share_to_clone.access_tier,
        protocols=share_to_clone.protocols,
        root_squash=share_to_clone.root_squash,
    )

    try:
        # [START set_share_quota]
        # Set the quota for the share to 1GB
        share_client.set_share_quota(quota=share_to_clone.quota)
        # [END set_share_quota]

        # # [START set_share_metadata]
        # data = {"category": "test"}
        # share_client.set_share_metadata(metadata=data)
        # # [END set_share_metadata]

        # Get the metadata for the share
        properties = share_client.get_share_properties().metadata
        print(f"Share properties: {properties}")

    finally:
        # Delete the share
        # share.delete_share()
        print("NOT IMPLEMENTED")
    share_client.close()


def file_service_properties(connection_string: str):
    # Instantiate the ShareServiceClient from a connection string
    from azure.storage.fileshare import ShareServiceClient

    file_service = ShareServiceClient.from_connection_string(connection_string)
    properties = file_service.get_service_properties()
    return properties


def get_share_endpoint(connection_string: str, file_name: str) -> str:
    from azure.storage.fileshare import ShareServiceClient

    file_service = ShareServiceClient.from_connection_string(connection_string)

    share = file_service.get_share_client(file_name)
    return share.url


def list_shares_in_service(connection_string: str):
    # Instantiate the ShareServiceClient from a connection string
    from azure.storage.fileshare import ShareServiceClient

    file_service = ShareServiceClient.from_connection_string(connection_string)

    try:
        # [START fsc_list_shares]
        # List the shares in the file service
        my_shares = list(file_service.list_shares())

        # # Print the shares
        # for share in my_shares:
        #     print(share)
        # # [END fsc_list_shares]
    except Exception as e:
        print(f"Error listing shares: {e}")
    return my_shares


###====================
### LOGGING CONFIGURATION
###====================
log_level_str = os.getenv("REPLICA_LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)

logging.basicConfig(
    level=log_level,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("replica_file_share.log", mode="w"),
    ],
)

### Azure SDK logs separati (default WARNING)
azure_log_level_str = os.getenv("AZURE_LOG_LEVEL", "WARNING").upper()
azure_log_level = getattr(logging, azure_log_level_str, logging.WARNING)
logging.getLogger("azure").setLevel(azure_log_level)

logging.info(
    f"Logging level set to {log_level_str}, Azure SDK log level {azure_log_level_str}"
)


### CONFIGURAZIONE
SOURCE_CONNECTION_STRING = os.getenv("AZURE_SOURCE_CONNECTION_STRING_FILE_SHARE", None)
DEST_CONNECTION_STRING = os.getenv("AZURE_DEST_CONNECTION_STRING_FILE_SHARE", None)
SOURCE_SAS_TOKEN = os.getenv("AZURE_SOURCE_TOKEN_SAS", None)
DEST_SAS_TOKEN = os.getenv("AZURE_DEST_TOKEN_SAS", None)


if not all(
    [SOURCE_CONNECTION_STRING, DEST_CONNECTION_STRING, SOURCE_SAS_TOKEN, DEST_SAS_TOKEN]
):
    logging.error(
        "All environment variables must be set: SOURCE_CONNECTION_STRING, DEST_CONNECTION_STRING, SOURCE_SAS_TOKEN, DEST_SAS_TOKEN."
    )
    sys.exit(1)


source_shares = list_shares_in_service(SOURCE_CONNECTION_STRING)
print("Listing file shares:")
for share in source_shares:
    print(f"input share {share}")
    create_share_with_quota_and_metadata(DEST_CONNECTION_STRING, share)
    source_endpoint = get_share_endpoint(
        connection_string=SOURCE_CONNECTION_STRING, file_name=share.name
    )
    destination_endpoint = get_share_endpoint(
        connection_string=DEST_CONNECTION_STRING, file_name=share.name
    )
    src = f"{source_endpoint}?{SOURCE_SAS_TOKEN}"
    dest = f"{destination_endpoint}?{DEST_SAS_TOKEN}"
    run_azcopy_sync(source_sas_url=src, dest_sas_url=dest)
