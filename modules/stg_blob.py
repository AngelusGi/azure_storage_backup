import sys
import logging
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from azure.core.exceptions import (
    HttpResponseError,
    ResourceNotFoundError,
)
from typing import Tuple


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


class BlobReplicator:
    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        source_account: str,
        dest_account: str,
        overwrite: bool = False,
        check_modification_time: bool = True,
        check_size: bool = True,
        check_content_md5: bool = True,
    ):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.source_account = source_account
        self.dest_account = dest_account
        self.overwrite = overwrite
        self.check_modification_time = check_modification_time
        self.check_size = check_size
        self.check_content_md5 = check_content_md5
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

    def blob_needs_copy(
        self,
        source_blob_client: BlobClient,
        dest_blob_client: BlobClient,
        blob_name: str,
    ) -> Tuple[bool, str]:
        """
        Determines if a blob needs to be copied based on various criteria.
        Returns (needs_copy: bool, reason: str)
        """
        try:
            # Get destination blob properties
            dest_properties = dest_blob_client.get_blob_properties()
        except ResourceNotFoundError:
            return True, "destination blob does not exist"
        except Exception as e:
            logging.warning(
                f"Could not get properties for destination blob '{blob_name}': {e}"
            )
            return True, f"could not verify destination blob properties: {e}"

        try:
            # Get source blob properties
            source_properties = source_blob_client.get_blob_properties()
        except Exception as e:
            logging.error(
                f"Could not get properties for source blob '{blob_name}': {e}"
            )
            return False, f"could not get source blob properties: {e}"

        # Check Content-MD5 first (most reliable indicator of content changes)
        if self.check_content_md5:
            source_md5 = (
                source_properties.content_settings.content_md5
                if source_properties.content_settings
                else None
            )
            dest_md5 = (
                dest_properties.content_settings.content_md5
                if dest_properties.content_settings
                else None
            )

            # If both have MD5, compare them
            if source_md5 and dest_md5:
                if source_md5 != dest_md5:
                    return (
                        True,
                        f"content MD5 mismatch (source: {source_md5.hex() if source_md5 else None}, dest: {dest_md5.hex() if dest_md5 else None})",
                    )
            # If only one has MD5, assume they're different
            elif source_md5 or dest_md5:
                return (
                    True,
                    f"MD5 availability mismatch (source: {'yes' if source_md5 else 'no'}, dest: {'yes' if dest_md5 else 'no'})",
                )

        # Check size
        if self.check_size:
            if source_properties.size != dest_properties.size:
                return (
                    True,
                    f"size mismatch (source: {source_properties.size}, dest: {dest_properties.size})",
                )

        # Check last modified time
        if self.check_modification_time:
            if source_properties.last_modified > dest_properties.last_modified:
                return (
                    True,
                    f"source is newer (source: {source_properties.last_modified}, dest: {dest_properties.last_modified})",
                )

        return False, "blob is up to date"

    def replicate(self) -> None:
        logging.info(f"Source Blob Storage URL: {self.source_url}")
        logging.info(f"Destination Blob Storage URL: {self.dest_url}")
        logging.info(f"Overwrite setting: {self.overwrite}")
        logging.info(
            f"Incremental replication enabled with checks - Content-MD5: {self.check_content_md5}, Size: {self.check_size}, ModTime: {self.check_modification_time}"
        )

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

        total_copied = 0
        total_skipped = 0
        total_errors = 0

        for container in source_containers:
            container_name = container.name
            logging.info(f"Processing container: '{container_name}'")

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
                total_errors += 1
                continue

            # Ensure destination container exists
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
                    total_errors += 1
                    continue
            except Exception as e:
                logging.error(
                    f"Could not ensure container '{container_name}' in '{self.dest_url}': {e}"
                )
                self.errors.append((container_name, str(e)))
                total_errors += 1
                continue

            try:
                blobs = list(source_container_client.list_blobs())
                total_blobs = len(blobs)

                if total_blobs == 0:
                    logging.info(f"No blobs found in container '{container_name}'")
                    continue

                container_copied = 0
                container_skipped = 0
                container_errors = 0
                max_retries = 3
                retry_delay = 5

                logging.info(
                    f"Found {total_blobs} blobs in container '{container_name}'"
                )

                for i, blob in enumerate(blobs, 1):
                    blob_name = blob.name

                    for attempt in range(1, max_retries + 1):
                        try:
                            source_blob_client: BlobClient = (
                                source_container_client.get_blob_client(blob_name)
                            )
                            dest_blob_client: BlobClient = (
                                dest_container_client.get_blob_client(blob_name)
                            )

                            # Check if blob needs to be copied
                            needs_copy, reason = self.blob_needs_copy(
                                source_blob_client, dest_blob_client, blob_name
                            )

                            if not needs_copy and not self.overwrite:
                                logging.info(
                                    f"[{container_name}] Skipping blob '{blob_name}' ({i}/{total_blobs}) - {reason}"
                                )
                                container_skipped += 1
                                break
                            elif needs_copy or self.overwrite:
                                if self.overwrite:
                                    copy_reason = "overwrite enabled"
                                else:
                                    copy_reason = reason

                                # Copy the blob
                                source_properties = (
                                    source_blob_client.get_blob_properties()
                                )
                                dest_blob_client.upload_blob_from_url(
                                    source_url=source_blob_client.url,
                                    metadata=source_properties.metadata,
                                    overwrite=True,  # Always overwrite when copying
                                )
                                container_copied += 1
                                logging.info(
                                    f"[{container_name}] Copied blob '{blob_name}' ({i}/{total_blobs}) - {copy_reason}"
                                )
                                break

                        except HttpResponseError as e:
                            if attempt < max_retries:
                                logging.warning(
                                    f"[{container_name}] Attempt {attempt}/{max_retries} failed for blob '{blob_name}': {e}. Retrying in {retry_delay}s..."
                                )
                                import time

                                time.sleep(retry_delay)
                            else:
                                logging.error(
                                    f"[{container_name}] Failed to process blob '{blob_name}' after {max_retries} attempts: {e}"
                                )
                                self.errors.append(
                                    (f"{container_name}/{blob_name}", str(e))
                                )
                                container_errors += 1
                        except Exception as e:
                            if attempt < max_retries:
                                logging.warning(
                                    f"[{container_name}] Attempt {attempt}/{max_retries} failed for blob '{blob_name}': {e}. Retrying in {retry_delay}s..."
                                )
                                import time

                                time.sleep(retry_delay)
                            else:
                                logging.error(
                                    f"[{container_name}] Failed to process blob '{blob_name}' after {max_retries} attempts: {e}"
                                )
                                self.errors.append(
                                    (f"{container_name}/{blob_name}", str(e))
                                )
                                container_errors += 1

                logging.info(
                    f"Container '{container_name}' completed: {container_copied} copied, {container_skipped} skipped, {container_errors} errors"
                )

                total_copied += container_copied
                total_skipped += container_skipped
                total_errors += container_errors

            except Exception as e:
                logging.error(
                    f"Error processing blobs in container '{container_name}': {e}"
                )
                self.errors.append((container_name, str(e)))
                total_errors += 1

        # Final summary
        logging.info(
            f"Replication completed: {total_copied} blobs copied, {total_skipped} blobs skipped, {total_errors} errors"
        )

        if self.errors:
            logging.warning("Replication completed with errors:")
            for error_item, error_content in self.errors:
                logging.warning(f"  '{error_item}' -> {error_content}")
        else:
            logging.info("Replication completed successfully without errors")
