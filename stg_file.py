import os
import sys
import logging
import subprocess
from azure.storage.fileshare import ShareClient
from typing import Optional


def setup_logging(log_file: str = "replica_file_share.log") -> None:
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
    logging.getLogger("azure").setLevel(azure_log_level)


class FileShareReplicator:
    def __init__(self, source_connection_string: str, dest_connection_string: str):
        self.source_connection_string = source_connection_string
        self.dest_connection_string = dest_connection_string
        self.errors = []

    def validate_env(self) -> bool:
        missing = []
        if not self.source_connection_string:
            missing.append("AZURE_SOURCE_CONNECTION_STRING_FILE_SHARE")
        if not self.dest_connection_string:
            missing.append("AZURE_DEST_CONNECTION_STRING_FILE_SHARE")
        if missing:
            logging.error(f"Missing environment variables: {', '.join(missing)}")
            return False
        return True

    def list_shares_in_service(self, connection_string: str):
        from azure.storage.fileshare import ShareServiceClient

        file_service = ShareServiceClient.from_connection_string(connection_string)
        try:
            my_shares = list(file_service.list_shares())
        except Exception as e:
            logging.error(f"Error listing shares: {e}")
            sys.exit(1)
        return my_shares

    def get_share_endpoint(self, connection_string: str, file_name: str) -> str:
        from azure.storage.fileshare import ShareServiceClient

        file_service = ShareServiceClient.from_connection_string(connection_string)
        share = file_service.get_share_client(file_name)
        return share.url

    def create_share_with_quota_and_metadata(
        self, connection_string_destination_storage: str, share_to_clone
    ):
        share_name: str = share_to_clone.name
        share_client = ShareClient.from_connection_string(
            conn_str=connection_string_destination_storage, share_name=share_name
        )
        try:
            share_client.create_share(
                metadata=getattr(share_to_clone, "metadata", None)
            )
            quota = getattr(share_to_clone, "quota", None)
            if quota:
                logging.info(f"settings quota to {quota}")
                share_client.set_share_quota(quota=quota)
            logging.info(
                f"Share {share_name} created successfully with metadata and quota."
            )
        except Exception as e:
            logging.error(f"Error creating/updating share {share_name}: {e}")
        finally:
            share_client.close()

    def run_azcopy_sync(self, source_sas_url: str, dest_sas_url: str) -> bool:
        try:
            logging.info(
                f"Starting azcopy sync from {source_sas_url} to {dest_sas_url}"
            )
            cmd = ["azcopy", "sync", source_sas_url, dest_sas_url]
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
            for line in process.stdout:
                logging.info(line.strip())
            for line in process.stderr:
                logging.error(line.strip())
            process.wait()
            if process.returncode == 0:
                logging.info("azcopy sync completed successfully")
                return True
            else:
                logging.error(
                    f"azcopy sync failed with return code {process.returncode}"
                )
                return False
        except FileNotFoundError:
            logging.error(
                "azcopy command not found. Please ensure azcopy is installed and in PATH"
            )
            return False
        except Exception as e:
            logging.exception(f"Unexpected error during azcopy sync: {str(e)}")
            return False

    def replicate(self) -> None:
        if not self.validate_env():
            sys.exit(1)
        source_shares = self.list_shares_in_service(self.source_connection_string)
        logging.info("Listing file shares:")
        for share in source_shares:
            logging.info(f"Input share: {share}")
            self.create_share_with_quota_and_metadata(
                connection_string_destination_storage=self.dest_connection_string,
                share_to_clone=share,
            )
            source_endpoint = self.get_share_endpoint(
                self.source_connection_string, file_name=share.name
            )
            destination_endpoint = self.get_share_endpoint(
                self.dest_connection_string, file_name=share.name
            )
            logging.info(
                f"Running azcopy sync from '{source_endpoint}' to '{destination_endpoint}'"
            )
            result = self.run_azcopy_sync(
                source_sas_url=source_endpoint, dest_sas_url=destination_endpoint
            )
            if not result:
                logging.error(f"Cannot perform sync for share {share.name}.")
            logging.info(
                "--------------------------------------------------------------------------------"
            )
