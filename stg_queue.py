import os
import sys
import time
import logging
from azure.identity import ClientSecretCredential
from azure.storage.queue import QueueServiceClient


def enforce_storage_queue_url(url: str) -> str:
    """Normalize Queue Storage URL"""
    if not url.endswith(".queue.core.windows.net"):
        url = f"{url}.queue.core.windows.net"
    if not url.startswith("https://"):
        url = f"https://{url}"
    return url


### =======================
### LOGGING CONFIGURATION
### =======================
log_level_str = os.getenv("REPLICA_LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)

logging.basicConfig(
    level=log_level,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("replica_queues.log", mode="w"),
    ],
)

### Azure SDK logs separati (default WARNING)
azure_log_level_str = os.getenv("AZURE_LOG_LEVEL", "WARNING").upper()
azure_log_level = getattr(logging, azure_log_level_str, logging.WARNING)
logging.getLogger("azure").setLevel(azure_log_level)

logging.info(
    f"Logging level set to {log_level_str}, Azure SDK log level {azure_log_level_str}"
)


### =======================
### CONFIGURATION
### =======================
TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
SOURCE_ACCOUNT = os.getenv("AZURE_SOURCE_STORAGE_ACCOUNT_QUEUE", None)
DEST_ACCOUNT = os.getenv("AZURE_DESTINATION_STORAGE_ACCOUNT_QUEUE", None)

### =======================
### VALIDATE ENV VARIABLES
### =======================
if not all([TENANT_ID, CLIENT_ID, CLIENT_SECRET]):
    logging.critical(
        "Fatal error. Authentication environment variables must be set: AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET."
    )
    sys.exit(1)

if not all([SOURCE_ACCOUNT, DEST_ACCOUNT]):
    logging.critical(
        "Fatal error. Storage account environment variables must be set: AZURE_SOURCE_STORAGE_ACCOUNT_QUEUE, AZURE_DESTINATION_STORAGE_ACCOUNT_QUEUE."
    )
    sys.exit(1)

### =======================
### QUEUE STORAGE ENDPOINTS
### =======================
source_url = enforce_storage_queue_url(SOURCE_ACCOUNT)
logging.info(f"Source Queue Storage URL: {source_url}")
dest_url = enforce_storage_queue_url(DEST_ACCOUNT)
logging.info(f"Destination Queue Storage URL: {dest_url}")

### =======================
### AUTHENTICATION
### =======================
try:
    credential = ClientSecretCredential(
        tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET
    )
except Exception as e:
    logging.critical(f"Fatal error. Unable to authenticate: {e}")
    sys.exit(1)

### =======================
### CREATE SERVICE CLIENTS
### =======================
try:
    source_service = QueueServiceClient(account_url=source_url, credential=credential)
    dest_service = QueueServiceClient(account_url=dest_url, credential=credential)
except Exception as e:
    logging.critical(f"Fatal error. Unable to create queue service clients: {e}")
    sys.exit(1)


### =======================
### QUEUE REPLICATION
### =======================
errors = []

try:
    source_queues = list(source_service.list_queues())
    logging.info(
        f"Found {len(source_queues)} queues to replicate from '{source_url}'"
    )

    for queue in source_queues:
        queue_name = queue.name
        logging.info(f"Processing queue: {queue_name}")

        try:
            ### Create destination queue if not exists
            dest_queue_client = dest_service.get_queue_client(queue_name)
            dest_queue_client.create_queue()
            logging.info(
                f"Queue '{queue_name}' created or already exists in destination"
            )

            ### Get source queue messages
            source_queue_client = source_service.get_queue_client(queue_name)

            messages = source_queue_client.receive_messages(messages_per_page=32)
            total_copied = 0
            max_retries = 5
            retry_delay = 2

            for msg_batch in messages.by_page():
                for msg in msg_batch:
                    for attempt in range(1, max_retries + 1):
                        try:
                            dest_queue_client.send_message(msg.content)
                            total_copied += 1
                            break
                        except Exception as e:
                            if attempt < max_retries:
                                logging.warning(
                                    f"[{queue_name}] Attempt {attempt}/{max_retries} failed for message '{msg.id}': {e}. Retrying in {retry_delay}s..."
                                )
                                time.sleep(retry_delay)
                            else:
                                logging.error(
                                    f"[{queue_name}] Failed to send message '{msg.id}' after {max_retries} attempts: {e}"
                                )
                                errors.append((queue_name, str(e)))

            logging.info(
                f"Finished: {total_copied} messages copied into '{queue_name}'"
            )

        except Exception as e:
            logging.error(f"Error processing queue '{queue_name}': {e}")
            errors.append((queue_name, str(e)))

except Exception as e:
    logging.critical(f"Fatal error listing source queues: {e}")
    sys.exit(1)

### =======================
### FINAL SUMMARY
### =======================
if errors:
    logging.warning(f"Replication completed with {len(errors)} errors")
    for queue_name, err in errors:
        logging.warning(f"Queue '{queue_name}' -> {err}")
else:
    logging.info("Replication completed successfully without errors")
