import sys
import time
import logging
from azure.identity import ClientSecretCredential
from azure.data.tables import TableServiceClient, TableClient, UpdateMode
from itertools import islice
from collections import defaultdict


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
            missing += ["ARM_TENANT_ID", "ARM_CLIENT_ID", "ARM_CLIENT_SECRET"]
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

    def delete_existing_tables(self, table_names: list[str]) -> None:
        """Delete existing tables in destination. Azure takes ~1 minute to fully delete tables."""
        logging.info("Phase 1: Checking and deleting existing tables in destination")
        tables_to_delete = []

        # Check which tables exist in destination
        try:
            dest_tables = list(self.dest_service.list_tables())
            existing_table_names = {t.name for t in dest_tables}

            for table_name in table_names:
                if table_name in existing_table_names:
                    tables_to_delete.append(table_name)

        except Exception as e:
            logging.error(f"Error listing destination tables: {e}")
            return

        if not tables_to_delete:
            logging.info("No existing tables to delete in destination")
            return

        # Delete existing tables
        logging.info(
            f"Deleting {len(tables_to_delete)} existing tables: {tables_to_delete}"
        )
        for table_name in tables_to_delete:
            try:
                self.dest_service.delete_table(table_name)
                logging.info(f"Table '{table_name}' deleted from destination")
            except Exception as e:
                logging.error(f"Failed to delete table '{table_name}': {e}")
                self.errors.append((table_name, f"deletion failed: {e}"))

        if tables_to_delete:
            logging.info("Waiting 60 seconds for Azure to fully delete tables...")
            time.sleep(60)

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
            source_tables_list = list(source_tables)  # Materializza la lista
            table_names = [table.name for table in source_tables_list]

            # Phase 1: Delete existing tables
            self.delete_existing_tables(table_names)

            # Phase 2: Create and copy tables
            logging.info("Phase 2: Creating and copying tables")
        except Exception as e:
            logging.critical(f"Error retrieving tables from {self.source_url}: {e}")
            sys.exit(1)
        for table in source_tables_list:
            table_name = table.name
            logging.info(f"Replicating table: '{table_name}'")
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
                self.dest_service.create_table(table_name)
                logging.info(f"Table '{table_name}' created in '{self.dest_url}'")
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
                        max_retries = 10
                        retry_delay = 10
                        for attempt in range(1, max_retries + 1):
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
                                break
                            except Exception as be:
                                if attempt < max_retries:
                                    logging.warning(
                                        f"[Table: {table_name} PK: {pk}] Attempt {attempt}/{max_retries} failed for batch: {be}. Retrying in {retry_delay}s..."
                                    )
                                    import time

                                    time.sleep(retry_delay)
                                else:
                                    logging.error(
                                        f"Batch error in table '{table_name}' PartitionKey='{pk}' after {max_retries} attempts: {be}"
                                    )
                                    self.errors.append((table_name, str(be)))
                logging.info(
                    f"Finished: {copied}/{total} entities copied into '{table_name}'"
                )
            except Exception as e:
                logging.error(f"Error copying entities from table '{table_name}': {e}")
                self.errors.append((table_name, str(e)))
        if self.errors:
            logging.warning("Replica completed with errors")
            for error_table, error_content in self.errors:
                logging.warning(f"Table '{error_table}' -> Error: {error_content}")
        else:
            logging.info("Replication completed successfully without errors")
