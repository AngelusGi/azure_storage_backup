import os
import sys
import logging
from azure.identity import ClientSecretCredential
from azure.data.tables import TableServiceClient, TableClient, UpdateMode
from itertools import islice
from collections import defaultdict

# =======================
# LOGGING CONFIGURATION
# =======================
# Default log level for the script is INFO, can be overridden by environment variable REPLICA_LOG_LEVEL
log_level_str = os.getenv("REPLICA_LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)

logging.basicConfig(
    level=log_level,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("table_replica_test_split_INFO.log", mode="w"),
    ],
)

logging.info(f"Logging level set to {log_level_str}")

# =======================
# AZURE SDK LOGGING CONFIGURATION
# =======================
# Set Azure SDK loggers to WARNING by default, can be overridden by environment variable AZURE_LOG_LEVEL
azure_log_level_str = os.getenv("AZURE_LOG_LEVEL", "WARNING").upper()
azure_log_level = getattr(logging, azure_log_level_str, logging.WARNING)

azure_loggers = [
    "azure",  # base Azure SDK logger
    "azure.core.pipeline",  # network/http logs
    "azure.identity",  # credential/authentication logs
    "azure.data.tables",  # table storage logs
]

for logger_name in azure_loggers:
    logger = logging.getLogger(logger_name)
    logger.setLevel(azure_log_level)

logging.info(f"Azure SDK loggers set to {azure_log_level_str}")

# =======================
# CONFIGURATION
# =======================
TENANT_ID = os.getenv("AZURE_TENANT_ID", None)
CLIENT_ID = os.getenv("AZURE_CLIENT_ID", None)
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET", None)

SOURCE_ACCOUNT = os.getenv("AZURE_SOURCE_STORAGE_ACCOUNT_TABLE", None)
DEST_ACCOUNT = os.getenv("AZURE_DESTINATION_STORAGE_ACCOUNT_TABLE", None)


def enforce_storage_table_url(url: str) -> str:
    """Normalize Table Storage URL"""
    if not url.endswith(".table.core.windows.net"):
        url = f"{url}.table.core.windows.net"
    if not url.startswith("https://"):
        url = f"https://{url}"
    return url


# =======================
# VALIDATE ENV VARIABLES
# =======================
if not all([TENANT_ID, CLIENT_ID, CLIENT_SECRET]):
    logging.error(
        "Authentication environment variables must be set: AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET."
    )
    sys.exit(1)

if not all([SOURCE_ACCOUNT, DEST_ACCOUNT]):
    logging.error(
        "Storage account environment variables must be set: AZURE_SOURCE_STORAGE_ACCOUNT_TABLE, AZURE_DESTINATION_STORAGE_ACCOUNT_TABLE."
    )
    sys.exit(1)

# =======================
# TABLE STORAGE ENDPOINTS
# =======================
source_url = enforce_storage_table_url(SOURCE_ACCOUNT)
logging.info(f"Source Table Storage URL: {source_url}")
dest_url = enforce_storage_table_url(DEST_ACCOUNT)
logging.info(f"Destination Table Storage URL: {dest_url}")

# =======================
# AUTHENTICATION
# =======================
try:
    credential = ClientSecretCredential(
        tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET
    )
except Exception as e:
    logging.critical(f"Unable to authenticate: {e}")
    sys.exit(1)

# =======================
# CREATE SERVICE CLIENTS
# =======================
try:
    source_service = TableServiceClient(endpoint=source_url, credential=credential)
    dest_service = TableServiceClient(endpoint=dest_url, credential=credential)
except Exception as e:
    logging.critical(f"Fatal error. Unable to create table service clients: {e}")
    sys.exit(1)

# =======================
# RETRIEVE TABLES
# =======================
try:
    logging.info(f"Retrieving list of tables from {source_url}...")
    source_tables = source_service.list_tables()
except Exception as e:
    logging.critical(f"Fatal error. Error retrieving tables from {source_url}: {e}")
    sys.exit(1)

# =======================
# ERROR REPORT
# =======================
errors = []


# =======================
# UTILITY: CHUNK ITERABLE
# =======================
def chunk(iterable, size=100):
    """Yield successive chunks of given size from iterable"""
    it = iter(iterable)
    while True:
        batch = list(islice(it, size))
        if not batch:
            break
        yield batch


# =======================
# REPLICATE TABLES
# =======================
for table in source_tables:
    table_name = table.name
    logging.info(f"--- Replicating table: '{table_name}' ---")

    # Create clients
    try:
        source_table_client: TableClient = source_service.get_table_client(
            table_name=table_name
        )
        dest_table_client: TableClient = dest_service.get_table_client(
            table_name=table_name
        )
    except Exception as e:
        logging.error(f"Error creating table clients for '{table_name}': {e}")
        errors.append((table_name, str(e)))
        continue

    # Ensure destination table exists (idempotent)
    try:
        dest_service.create_table_if_not_exists(table_name)
        logging.info(f"Table '{table_name}' ensured in '{dest_url}'")
    except Exception as e:
        logging.error(f"Could not ensure table '{table_name}' in '{dest_url}': {e}")
        errors.append((table_name, str(e)))
        continue

    # Copy entities
    try:
        entities = list(source_table_client.list_entities())
        total = len(entities)
        copied = 0

        # Group entities by PartitionKey to comply with batch rules
        partition_groups = defaultdict(list)
        for entity in entities:
            partition_groups[entity["PartitionKey"]].append(entity)

        # Submit batches per PartitionKey
        for pk, pk_entities in partition_groups.items():
            for batch in chunk(pk_entities, size=100):
                try:
                    dest_table_client.submit_transaction(
                        [("upsert", e, {"mode": UpdateMode.MERGE}) for e in batch]
                    )
                    copied += len(batch)
                    logging.info(
                        f"Copied {copied}/{total} entities into '{table_name}'"
                    )
                except Exception as be:
                    logging.error(
                        f"Batch error in table '{table_name}' PartitionKey='{pk}': {be}"
                    )
                    errors.append((table_name, str(be)))

        logging.info(f"Finished: {copied}/{total} entities copied into '{table_name}'")

    except Exception as e:
        logging.error(f"Error copying entities from table '{table_name}': {e}")
        errors.append((table_name, str(e)))

# =======================
# FINAL REPORT
# =======================
if errors:
    logging.warning("--- Replica completed with errors ---")
    for error_table, error_content in errors:
        logging.warning(f"Table '{error_table}' -> Error: {error_content}")
else:
    logging.info("--- Replica completed successfully without errors ---")
