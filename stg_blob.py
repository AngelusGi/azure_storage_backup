import os
import sys
import logging
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from azure.core.exceptions import ClientAuthenticationError, HttpResponseError
from typing import List, Optional


def enforce_storage_blob_url(url: str) -> str:
    if (
        url.find(".file.core.windows.net") != -1
        or url.find(".queue.core.windows.net") != -1
        or url.find(".table.core.windows.net") != -1
    ):
        logging.critical(f"Provided url is not valid for table. Provided url {url}")
        sys.exit(1)
    if url.endswith("/"):
        url = url[:-1]
    if not url.endswith(".blob.core.windows.net"):
        url = f"{url}.blob.core.windows.net"
    if not url.startswith("https://"):
        url = f"https://{url}"
    return url


def setup_logging(log_file: str = "blob_replica.log") -> None:
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
        "azure.storage.blob",
    ]
    for logger_name in azure_loggers:
        logger = logging.getLogger(logger_name)
        logger.setLevel(azure_log_level)


class BlobReplicator:
    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        source_account: str,
        dest_account: str,
        overwrite: bool = False,
    ):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.source_account = source_account
        self.dest_account = dest_account
        self.overwrite = overwrite
        self.errors = []
        if not self.validate_env():
            sys.exit(1)
        self.source_url = enforce_storage_blob_url(self.source_account)
        self.dest_url = enforce_storage_blob_url(self.dest_account)
        self.credential = None
        self.source_service = None
        self.dest_service = None

    def validate_env(self) -> bool:
        missing = []
        if not all([self.tenant_id, self.client_id, self.client_secret]):
            missing += ["ARM_TENANT_ID", "ARM_CLIENT_ID", "ARM_CLIENT_SECRET"]
        if not all([self.source_account, self.dest_account]):
            missing += [
                "AZURE_SOURCE_STORAGE_ACCOUNT_BLOB",
                "AZURE_DESTINATION_STORAGE_ACCOUNT_BLOB",
            ]
        if missing:
            logging.critical(f"Missing environment variables: {', '.join(missing)}")
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
            self.source_service = BlobServiceClient(
                account_url=self.source_url, credential=self.credential
            )
            self.dest_service = BlobServiceClient(
                account_url=self.dest_url, credential=self.credential
            )
            return True
        except Exception as e:
            logging.critical(f"Unable to create blob service clients: {e}")
            return False

    def replicate(self) -> None:
        logging.info(f"Source Blob Storage URL: {self.source_url}")
        logging.info(f"Destination Blob Storage URL: {self.dest_url}")
        logging.info(f"Overwrite setting: {self.overwrite}")
        if not self.authenticate():
            sys.exit(1)
        if not self.create_clients():
            sys.exit(1)
        try:
            logging.info(f"Retrieving list of containers from {self.source_url}...")
            source_containers = self.source_service.list_containers(
                include_metadata=True
            )
        except Exception as e:
            logging.critical(f"Error retrieving containers from {self.source_url}: {e}")
            sys.exit(1)
        for container in source_containers:
            container_name = container.name
            logging.info(f"--- Replicating container: '{container_name}' ---")
            try:
                source_container_client: ContainerClient = (
                    self.source_service.get_container_client(container_name)
                )
                dest_container_client: ContainerClient = (
                    self.dest_service.get_container_client(container_name)
                )
            except Exception as e:
                logging.error(
                    f"Error creating container clients for '{container_name}': {e}"
                )
                self.errors.append((container_name, str(e)))
                continue
            try:
                self.dest_service.create_container(container_name)
                logging.info(
                    f"Container '{container_name}' created in '{self.dest_url}'"
                )
            except HttpResponseError as e:
                if e.status_code == 409:
                    logging.info(
                        f"Container '{container_name}' already exists in '{self.dest_url}'"
                    )
                else:
                    logging.error(
                        f"Could not ensure container '{container_name}' in '{self.dest_url}': {e}"
                    )
                    self.errors.append((container_name, str(e)))
                    continue
            except Exception as e:
                logging.error(
                    f"Could not ensure container '{container_name}' in '{self.dest_url}': {e}"
                )
                self.errors.append((container_name, str(e)))
                continue
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
                        source_blob_client: BlobClient = (
                            source_container_client.get_blob_client(blob_name)
                        )
                        dest_blob_client: BlobClient = (
                            dest_container_client.get_blob_client(blob_name)
                        )
                        source_properties = source_blob_client.get_blob_properties()
                        dest_blob_client.upload_blob_from_url(
                            source_url=source_blob_client.url,
                            metadata=source_properties.metadata,
                            overwrite=self.overwrite,
                        )
                        copied += 1
                        logging.info(
                            f"Copied {copied}/{total} blobs into '{container_name}' - '{blob_name}'"
                        )
                    except HttpResponseError as e:
                        if e.status_code == 409 and not self.overwrite:
                            logging.warning(
                                f"Blob '{blob_name}' already exists in '{container_name}' (overwrite disabled)"
                            )
                            copied += 1
                        else:
                            logging.error(
                                f"Error copying blob '{blob_name}' in container '{container_name}': {e}"
                            )
                            self.errors.append(
                                (f"{container_name}/{blob_name}", str(e))
                            )
                    except Exception as e:
                        logging.error(
                            f"Error copying blob '{blob_name}' in container '{container_name}': {e}"
                        )
                        self.errors.append((f"{container_name}/{blob_name}", str(e)))
                logging.info(
                    f"Finished: {copied}/{total} blobs copied into '{container_name}'"
                )
            except Exception as e:
                logging.error(
                    f"Error copying blobs from container '{container_name}': {e}"
                )
                self.errors.append((container_name, str(e)))
        if self.errors:
            logging.warning("--- Replica completed with errors ---")
            for error_item, error_content in self.errors:
                logging.warning(f"Item '{error_item}' -> Error: {error_content}")
        else:
            logging.info("--- Replica completed successfully without errors ---")
