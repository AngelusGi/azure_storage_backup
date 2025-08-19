import os
import sys
import logging
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from azure.core.exceptions import ClientAuthenticationError, HttpResponseError
from typing import List


def enforce_storage_blob_url(url: str) -> str:
    """Normalize Blob Storage URL"""
    if not url.endswith(".blob.core.windows.net"):
        url = f"{url}.blob.core.windows.net"
    if not url.startswith("https://"):
        url = f"https://{url}"
    return url


### =======================
### LOGGING CONFIGURATION
### =======================
### Default log level for the script is INFO, can be overridden by environment variable REPLICA_LOG_LEVEL
log_level_str = os.getenv("REPLICA_LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)

logging.basicConfig(
    level=log_level,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("blob_replica.log", mode="w"),
    ],
)

logging.info(f"Logging level set to {log_level_str}")

### =======================
### AZURE SDK LOGGING CONFIGURATION
### =======================
### Set Azure SDK loggers to WARNING by default, can be overridden by environment variable AZURE_LOG_LEVEL
azure_log_level_str = os.getenv("AZURE_LOG_LEVEL", "WARNING").upper()
azure_log_level = getattr(logging, azure_log_level_str, logging.WARNING)

azure_loggers = [
    "azure",  ### base Azure SDK logger
    "azure.core.pipeline",  ### network/http logs
    "azure.identity",  ### credential/authentication logs
    "azure.storage.blob",  ### blob storage logs
]

for logger_name in azure_loggers:
    logger = logging.getLogger(logger_name)
    logger.setLevel(azure_log_level)

logging.info(f"Azure SDK loggers set to {azure_log_level_str}")

### =======================
### CONFIGURATION
### =======================
TENANT_ID = os.getenv("AZURE_TENANT_ID", None)
CLIENT_ID = os.getenv("AZURE_CLIENT_ID", None)
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET", None)

SOURCE_ACCOUNT = os.getenv("AZURE_SOURCE_STORAGE_ACCOUNT_BLOB", None)
DEST_ACCOUNT = os.getenv("AZURE_DESTINATION_STORAGE_ACCOUNT_BLOB", None)
OVERWRITE = os.getenv("OVERWRITE_STORAGE_ACCOUNT_BLOB", "False").lower() == "true"

### =======================
### VALIDATE ENV VARIABLES
### =======================
if not all([TENANT_ID, CLIENT_ID, CLIENT_SECRET]):
    logging.error(
        "Authentication environment variables must be set: AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET."
    )
    sys.exit(1)

if not all([SOURCE_ACCOUNT, DEST_ACCOUNT]):
    logging.error(
        "Storage account environment variables must be set: AZURE_SOURCE_STORAGE_ACCOUNT_BLOB, AZURE_DESTINATION_STORAGE_ACCOUNT_BLOB."
    )
    sys.exit(1)

### =======================
### BLOB STORAGE ENDPOINTS
### =======================
source_url = enforce_storage_blob_url(SOURCE_ACCOUNT)
logging.info(f"Source Blob Storage URL: {source_url}")
dest_url = enforce_storage_blob_url(DEST_ACCOUNT)
logging.info(f"Destination Blob Storage URL: {dest_url}")
logging.info(f"Overwrite setting: {OVERWRITE}")

### =======================
### AUTHENTICATION
### =======================
try:
    credential = ClientSecretCredential(
        tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET
    )
except Exception as e:
    logging.critical(f"Unable to authenticate: {e}")
    sys.exit(1)

### =======================
### CREATE SERVICE CLIENTS
### =======================
try:
    source_service = BlobServiceClient(account_url=source_url, credential=credential)
    dest_service = BlobServiceClient(account_url=dest_url, credential=credential)
except Exception as e:
    logging.critical(f"Fatal error. Unable to create blob service clients: {e}")
    sys.exit(1)

### =======================
### RETRIEVE CONTAINERS
### =======================
try:
    logging.info(f"Retrieving list of containers from {source_url}...")
    source_containers = source_service.list_containers(include_metadata=True)
except Exception as e:
    logging.critical(f"Fatal error. Error retrieving containers from {source_url}: {e}")
    sys.exit(1)

### =======================
### ERROR REPORT
### =======================
errors = []

### =======================
### REPLICATE CONTAINERS AND BLOBS
### =======================
for container in source_containers:
    container_name = container.name
    logging.info(f"--- Replicating container: '{container_name}' ---")

    ### Create container clients
    try:
        source_container_client: ContainerClient = source_service.get_container_client(
            container_name
        )
        dest_container_client: ContainerClient = dest_service.get_container_client(
            container_name
        )
    except Exception as e:
        logging.error(f"Error creating container clients for '{container_name}': {e}")
        errors.append((container_name, str(e)))
        continue

    ### Ensure destination container exists (idempotent)
    try:
        dest_service.create_container(container_name)
        logging.info(f"Container '{container_name}' created in '{dest_url}'")
    except HttpResponseError as e:
        if e.status_code == 409:  ### Container already exists
            logging.info(f"Container '{container_name}' already exists in '{dest_url}'")
        else:
            logging.error(
                f"Could not ensure container '{container_name}' in '{dest_url}': {e}"
            )
            errors.append((container_name, str(e)))
            continue
    except Exception as e:
        logging.error(
            f"Could not ensure container '{container_name}' in '{dest_url}': {e}"
        )
        errors.append((container_name, str(e)))
        continue

    ### Copy blobs
    try:
        blobs = list(source_container_client.list_blobs())
        total = len(blobs)

        if total == 0:
            logging.info(f"No blobs found in container '{container_name}'")
            continue

        copied = 0

        for blob in blobs:
            blob_name = blob.name
            try:
                ### Get source and destination blob clients
                source_blob_client: BlobClient = (
                    source_container_client.get_blob_client(blob_name)
                )
                dest_blob_client: BlobClient = dest_container_client.get_blob_client(
                    blob_name
                )

                ### Get blob metadata
                source_properties = source_blob_client.get_blob_properties()

                ### Copy blob
                dest_blob_client.upload_blob_from_url(
                    source_url=source_blob_client.url,
                    metadata=source_properties.metadata,
                    overwrite=OVERWRITE,
                )

                copied += 1
                logging.info(
                    f"Copied {copied}/{total} blobs into '{container_name}' - '{blob_name}'"
                )

            except HttpResponseError as e:
                if e.status_code == 409 and not OVERWRITE:
                    logging.warning(
                        f"Blob '{blob_name}' already exists in '{container_name}' (overwrite disabled)"
                    )
                    copied += 1
                else:
                    logging.error(
                        f"Error copying blob '{blob_name}' in container '{container_name}': {e}"
                    )
                    errors.append((f"{container_name}/{blob_name}", str(e)))
            except Exception as e:
                logging.error(
                    f"Error copying blob '{blob_name}' in container '{container_name}': {e}"
                )
                errors.append((f"{container_name}/{blob_name}", str(e)))

        logging.info(f"Finished: {copied}/{total} blobs copied into '{container_name}'")

    except Exception as e:
        logging.error(f"Error copying blobs from container '{container_name}': {e}")
        errors.append((container_name, str(e)))

### =======================
### FINAL REPORT
### =======================
if errors:
    logging.warning("--- Replica completed with errors ---")
    for error_item, error_content in errors:
        logging.warning(f"Item '{error_item}' -> Error: {error_content}")
else:
    logging.info("--- Replica completed successfully without errors ---")
