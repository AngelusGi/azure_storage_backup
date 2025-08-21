import sys
import time
import logging
from azure.identity import ClientSecretCredential
from azure.storage.queue import QueueServiceClient


def enforce_storage_queue_url(url: str) -> str:
    if (
        url.find(".table.core.windows.net") != -1
        or url.find(".blob.core.windows.net") != -1
        or url.find(".file.core.windows.net") != -1
    ):
        logging.critical(f"Provided url is not valid for table. Provided url {url}")
        sys.exit(1)
    if url.endswith("/"):
        url = url[:-1]
    if not url.endswith(".queue.core.windows.net"):
        url = f"{url}.queue.core.windows.net"
    if not url.startswith("https://"):
        url = f"https://{url}"
    return url


class QueueReplicator:
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
        self.source_url = enforce_storage_queue_url(self.source_account)
        self.dest_url = enforce_storage_queue_url(self.dest_account)
        self.credential = None
        self.source_service = None
        self.dest_service = None

    def validate_env(self) -> bool:
        missing = []
        if not all([self.tenant_id, self.client_id, self.client_secret]):
            missing += ["ARM_TENANT_ID", "ARM_CLIENT_ID", "ARM_CLIENT_SECRET"]
        if not all([self.source_account, self.dest_account]):
            missing += [
                "AZURE_SOURCE_STORAGE_ACCOUNT_QUEUE",
                "AZURE_DESTINATION_STORAGE_ACCOUNT_QUEUE",
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
            self.source_service = QueueServiceClient(
                account_url=self.source_url, credential=self.credential
            )
            self.dest_service = QueueServiceClient(
                account_url=self.dest_url, credential=self.credential
            )
            return True
        except Exception as e:
            logging.critical(f"Unable to create queue service clients: {e}")
            return False

    def replicate(self) -> None:
        logging.info(f"Source Queue Storage URL: {self.source_url}")
        logging.info(f"Destination Queue Storage URL: {self.dest_url}")
        if not self.authenticate():
            sys.exit(1)
        if not self.create_clients():
            sys.exit(1)
        try:
            source_queues = list(self.source_service.list_queues())
            logging.info(
                f"Found {len(source_queues)} queues to replicate from '{self.source_url}'"
            )
            for queue in source_queues:
                queue_name = queue.name
                logging.info(f"Processing queue: {queue_name}")
                try:
                    dest_queue_client = self.dest_service.get_queue_client(queue_name)
                    dest_queue_client.create_queue()
                    logging.info(
                        f"Queue '{queue_name}' created or already exists in destination"
                    )
                    source_queue_client = self.source_service.get_queue_client(
                        queue_name
                    )
                    messages = source_queue_client.receive_messages(
                        messages_per_page=32
                    )
                    total_copied = 0
                    max_retries = 10
                    retry_delay = 10
                    for msg_batch in messages.by_page():
                        for msg in msg_batch:
                            for attempt in range(1, max_retries + 1):
                                try:
                                    logging.debug(f"message content {msg}")
                                    if msg.expires_on.year == 9999:
                                        logging.debug("never expire message found.")
                                        expire_on = -1
                                    else:
                                        expire_on = int(msg.expires_on.timestamp())
                                        logging.debug(
                                            f"message will expire on {msg.expires_on} | converted to epoch time {expire_on}"
                                        )
                                    dest_queue_client.send_message(
                                        content=msg.content, time_to_live=expire_on
                                    )
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
                                        self.errors.append((queue_name, str(e)))
                    logging.info(
                        f"Finished: {total_copied} messages copied into '{queue_name}'"
                    )
                except Exception as e:
                    logging.error(f"Error processing queue '{queue_name}': {e}")
                    self.errors.append((queue_name, str(e)))
        except Exception as e:
            logging.critical(f"Fatal error listing source queues: {e}")
            sys.exit(1)
        if self.errors:
            logging.warning(f"Replication completed with {len(self.errors)} errors")
            for queue_name, err in self.errors:
                logging.warning(f"Queue '{queue_name}' -> {err}")
        else:
            logging.info("Replication completed successfully without errors")
