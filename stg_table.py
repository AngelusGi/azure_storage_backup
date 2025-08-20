from operator import contains
import os
import sys
import logging
from azure.identity import ClientSecretCredential
from azure.data.tables import TableServiceClient, TableClient, UpdateMode
from itertools import islice
from collections import defaultdict


def setup_logging(log_file: str = "table_replica.log") -> None:
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
    azure_log_level_str = os.getenv("AZURE_LOG_LEVEL", "WARNING").upper()
    azure_log_level = getattr(logging, azure_log_level_str, logging.WARNING)
    azure_loggers = [
        "azure",
        "azure.core.pipeline",
        "azure.identity",
        "azure.data.tables",
    ]
    for logger_name in azure_loggers:
        logger = logging.getLogger(logger_name)
        logger.setLevel(azure_log_level)


def enforce_storage_table_url(url: str) -> str:
    if (
        url.find(".file.core.windows.net") != -1
        or url.find(".blob.core.windows.net") != -1
        or url.find(".queue.core.windows.net") != -1
    ):
        logging.critical(f"Provided url is not valid for table. Provided url {url}")
        sys.exit(1)
    if url.endswith("/"):
        url = url[:-1]
    if not url.endswith(".table.core.windows.net"):
        url = f"{url}.table.core.windows.net"
    if not url.startswith("https://"):
        url = f"https://{url}"
    return url


def chunk(iterable, size=100):
    it = iter(iterable)
    while True:
        batch = list(islice(it, size))
        if not batch:
            break
        yield batch


class TableReplicator:
    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        source_account: str,
        dest_account: str,
    ):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.source_account = source_account
        self.dest_account = dest_account
        self.errors = []
        if not self.validate_env():
            sys.exit(1)
        self.source_url = enforce_storage_table_url(self.source_account)
        self.dest_url = enforce_storage_table_url(self.dest_account)
        self.credential = None
        self.source_service = None
        self.dest_service = None

    def validate_env(self) -> bool:
        missing = []
        if not all([self.tenant_id, self.client_id, self.client_secret]):
            missing += ["AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET"]
        if not all([self.source_account, self.dest_account]):
            missing += [
                "AZURE_SOURCE_STORAGE_ACCOUNT_TABLE",
                "AZURE_DESTINATION_STORAGE_ACCOUNT_TABLE",
            ]
        if missing:
            logging.error(f"Missing environment variables: {', '.join(missing)}")
            return False
        return True

    def authenticate(self) -> bool:
        try:
            self.credential = ClientSecretCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
            return True
        except Exception as e:
            logging.critical(f"Unable to authenticate: {e}")
            return False

    def create_clients(self) -> bool:
        try:
            self.source_service = TableServiceClient(
                endpoint=self.source_url, credential=self.credential
            )
            self.dest_service = TableServiceClient(
                endpoint=self.dest_url, credential=self.credential
            )
            return True
        except Exception as e:
            logging.critical(f"Unable to create table service clients: {e}")
            return False

    def replicate(self) -> None:
        logging.info(f"Source Table Storage URL: {self.source_url}")
        logging.info(f"Destination Table Storage URL: {self.dest_url}")
        if not self.authenticate():
            sys.exit(1)
        if not self.create_clients():
            sys.exit(1)
        try:
            logging.info(f"Retrieving list of tables from {self.source_url}...")
            source_tables = self.source_service.list_tables()
        except Exception as e:
            logging.critical(f"Error retrieving tables from {self.source_url}: {e}")
            sys.exit(1)
        for table in source_tables:
            table_name = table.name
            logging.info(f"--- Replicating table: '{table_name}' ---")
            try:
                source_table_client: TableClient = self.source_service.get_table_client(
                    table_name=table_name
                )
                dest_table_client: TableClient = self.dest_service.get_table_client(
                    table_name=table_name
                )
            except Exception as e:
                logging.error(f"Error creating table clients for '{table_name}': {e}")
                self.errors.append((table_name, str(e)))
                continue
            try:
                self.dest_service.create_table_if_not_exists(table_name)
                logging.info(f"Table '{table_name}' ensured in '{self.dest_url}'")
            except Exception as e:
                logging.error(
                    f"Could not ensure table '{table_name}' in '{self.dest_url}': {e}"
                )
                self.errors.append((table_name, str(e)))
                continue
            try:
                entities = list(source_table_client.list_entities())
                total = len(entities)
                copied = 0
                partition_groups = defaultdict(list)
                for entity in entities:
                    partition_groups[entity["PartitionKey"]].append(entity)
                for pk, pk_entities in partition_groups.items():
                    for batch in chunk(pk_entities, size=100):
                        try:
                            dest_table_client.submit_transaction(
                                [
                                    ("upsert", e, {"mode": UpdateMode.MERGE})
                                    for e in batch
                                ]
                            )
                            copied += len(batch)
                            logging.info(
                                f"Copied {copied}/{total} entities into '{table_name}'"
                            )
                        except Exception as be:
                            logging.error(
                                f"Batch error in table '{table_name}' PartitionKey='{pk}': {be}"
                            )
                            self.errors.append((table_name, str(be)))
                logging.info(
                    f"Finished: {copied}/{total} entities copied into '{table_name}'"
                )
            except Exception as e:
                logging.error(f"Error copying entities from table '{table_name}': {e}")
                self.errors.append((table_name, str(e)))
        if self.errors:
            logging.warning("--- Replica completed with errors ---")
            for error_table, error_content in self.errors:
                logging.warning(f"Table '{error_table}' -> Error: {error_content}")
        else:
            logging.info("--- Replica completed successfully without errors ---")
