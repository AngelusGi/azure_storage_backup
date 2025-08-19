import subprocess
import os
import logging
import sys
from azure.storage.fileshare import (
    ShareProperties,
)  # azure-storage-file-share
from azure.storage.fileshare import ShareClient
from azure.mgmt.storage import StorageManagementClient
from azure.identity import ClientSecretCredential, DefaultAzureCredential

connection_string = os.getenv("STORAGE_CONNECTION_STRING")

# Configurazione logging: console + file
logger = logging.getLogger("azcopy_sync")
logger.setLevel(logging.INFO)

# Stream handler (console)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)

# File handler (dedicato)
file_handler = logging.FileHandler("sync_file_share.log")
file_handler.setLevel(logging.INFO)

# Formatter comune
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# Aggiungi gli handler se non già presenti
if not logger.handlers:
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


def run_azcopy_sync(source_sas_url: str, dest_sas_url: str) -> bool:
    """Execute azcopy sync command with live log streaming and error handling"""
    try:
        logger.info(f"Starting azcopy sync from {source_sas_url} to {dest_sas_url}")

        # Build the azcopy command
        cmd = ["azcopy", "sync", source_sas_url, dest_sas_url]

        # Usa Popen per stream in tempo reale
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

        # Stream stdout
        for line in process.stdout:
            logger.info(line.strip())

        # Stream stderr
        for line in process.stderr:
            logger.error(line.strip())

        # Aspetta la fine del processo
        process.wait()

        if process.returncode == 0:
            logger.info("azcopy sync completed successfully")
            return True
        else:
            logger.error(f"azcopy sync failed with return code {process.returncode}")
            return False

    except FileNotFoundError:
        logger.error(
            "azcopy command not found. Please ensure azcopy is installed and in PATH"
        )
        return False
    except Exception as e:
        logger.exception(f"Unexpected error during azcopy sync: {str(e)}")
        return False


def create_share_with_quota_and_metadata(
    connection_string_destination_storage: str, share_to_clone
):
    """
    Clona una file share in un altro storage account:
    - Ricrea la share con metadata
    - Imposta quota via data-plane
    """

    share_name: str = share_to_clone.name
    share_client = ShareClient.from_connection_string(
        conn_str=connection_string_destination_storage, share_name=share_name
    )

    try:
        # Crea la share con metadata (data-plane)
        share_client.create_share(metadata=share_to_clone.metadata)

        # Imposta quota se esiste (data-plane)
        if share_to_clone.quota:
            print(f"settings quota to {share_to_clone.quota}")
            share_client.set_share_quota(quota=share_to_clone.quota)

        print(f"Share {share_name} created successfully with metadata and quota.")

    except Exception as e:
        print(f"Error creating/updating share {share_name}: {e}")
    finally:
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
        my_shares = list(file_service.list_shares())
    except Exception as e:
        print(f"Error listing shares: {e}")
        sys.exit(1)
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
    create_share_with_quota_and_metadata(
        connection_string_destination_storage=DEST_CONNECTION_STRING,
        share_to_clone=share,
    )
    source_endpoint = get_share_endpoint(
        connection_string=SOURCE_CONNECTION_STRING, file_name=share.name
    )
    destination_endpoint = get_share_endpoint(
        connection_string=DEST_CONNECTION_STRING, file_name=share.name
    )

    print(f"Running azcopy sync from '{source_endpoint}' to '{destination_endpoint}'")
    run_azcopy_sync(source_sas_url=source_endpoint, dest_sas_url=destination_endpoint)
