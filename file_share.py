from azure.core.exceptions import ClientAuthenticationError, HttpResponseError
from azure.identity import ClientSecretCredential
from azure.storage.fileshare import (
    ShareServiceClient,
    ShareClient,
    ShareDirectoryClient,
    ShareFileClient,
)
from typing import List, Dict, Any
import logging
import os
from datetime import datetime

logging.basicConfig(level=logging.WARNING)


def retrieve_storage_account() -> dict[str, str]:
    """
    Retrieve the storage account information from environment variables.
    """
    source = os.getenv("SOURCE_STORAGE_ACCOUNT_FILE", None)
    destination = os.getenv("DESTINATION_STORAGE_ACCOUNT_FILE", None)

    overwrite: str = os.getenv("OVERWRITE_STORAGE_ACCOUNT_FILE", "False")

    if not all([source, destination]):
        raise ValueError(
            "Environment variables SOURCE_STORAGE_ACCOUNT_FILE and DESTINATION_STORAGE_ACCOUNT_FILE must be set."
        )

    print(f"Overwrite setting is set to: {overwrite}")

    return {"source": source, "destination": destination, "overwrite": overwrite}


def authenticate_azure() -> ClientSecretCredential:
    """
    Authenticate using ClientSecretCredential.
    This assumes that the environment variables for Azure credentials are set.
    """
    tenant_id = os.getenv("AZURE_TENANT_ID", None)
    client_id = os.getenv("AZURE_CLIENT_ID", None)
    client_secret = os.getenv("AZURE_CLIENT_SECRET", None)

    if not all([tenant_id, client_id, client_secret]):
        raise ValueError(
            "Environment variables AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET must be set."
        )

    credential = ClientSecretCredential(
        tenant_id=str(tenant_id),
        client_id=str(client_id),
        client_secret=str(client_secret),
    )

    try:
        credential.get_token("https://management.azure.com/.default")
    except ClientAuthenticationError as e:
        print("Unable to authenticate to Azure:", e)
        raise ClientAuthenticationError()
    return credential


def copy_file_between_shares(
    source_file: ShareFileClient,
    destination_file: ShareFileClient,
    overwrite: bool = False,
):
    """
    Copy a file from source file share to destination file share
    """
    try:
        # Download file content from source
        source_data = source_file.download_file()
        file_content = source_data.readall()

        # Get source file properties for metadata
        source_properties = source_file.get_file_properties()

        # Upload to destination
        destination_file.upload_file(
            file_content, overwrite=overwrite, metadata=source_properties.metadata
        )
        print(f"✓ Successfully copied file: {source_file.file_name}")

    except HttpResponseError as e:
        print(f"Failed to copy file {source_file.file_name}: {e}")
        raise


def authenticate_file_share_storage(
    storage_account_url: str,
    credential: ClientSecretCredential | None,
) -> ShareServiceClient:
    """
    credential is an instance of ClientSecretCredential, or None.
    None if you want to use SAS token attached to storage_account_url.
    """
    return ShareServiceClient(account_url=storage_account_url, credential=credential)


def get_file_shares_in_storage(
    share_service_client: ShareServiceClient,
) -> List[Dict[str, Any]]:
    """
    Returns a list of file share objects from the storage account.
    Each object contains share properties including name, metadata, etc.
    """
    shares_list: List[Dict[str, Any]] = []

    try:
        shares_iterator = share_service_client.list_shares(include_metadata=True)
        # Convert iterator to a standard list
        for share in shares_iterator:
            shares_list.append(
                {
                    "name": share.name,
                    "metadata": share.metadata,
                    "last_modified": share.last_modified,
                    "quota": share.quota,
                }
            )
    except HttpResponseError as e:
        print(f"Error listing file shares: {e}")
        raise

    if len(shares_list) == 0:
        print(
            f"No file shares found in the storage account {share_service_client.account_name}."
        )
    else:
        print_file_shares(shares_list)

    return shares_list


def authenticate_file_share(
    share_service_client: ShareServiceClient, share_name: str
) -> ShareClient:
    """
    Authenticate to a specific file share in the storage account.
    """
    return share_service_client.get_share_client(share_name)


def get_files_and_directories_in_share(
    share_client: ShareClient, directory_path: str = ""
) -> Dict[str, List]:
    """
    Returns files and directories from the specified file share directory.
    Returns a dictionary with 'files' and 'directories' keys.
    """
    files_list = []
    directories_list = []

    try:
        directory_client = share_client.get_directory_client(directory_path)

        for item in directory_client.list_directories_and_files():
            if item.is_directory:
                directories_list.append(
                    {
                        "name": item.name,
                        "path": (
                            f"{directory_path}/{item.name}"
                            if directory_path
                            else item.name
                        ),
                    }
                )
            else:
                file_client = share_client.get_file_client(
                    f"{directory_path}/{item.name}" if directory_path else item.name
                )
                file_properties = file_client.get_file_properties()
                files_list.append(
                    {
                        "name": item.name,
                        "path": (
                            f"{directory_path}/{item.name}"
                            if directory_path
                            else item.name
                        ),
                        "size": file_properties.size,
                        "last_modified": file_properties.last_modified,
                        "metadata": file_properties.metadata,
                    }
                )

    except HttpResponseError as e:
        print(f"Error listing files in directory '{directory_path}': {e}")
        raise

    return {"files": files_list, "directories": directories_list}


def get_all_files_recursive(
    share_client: ShareClient, directory_path: str = ""
) -> List[Dict[str, Any]]:
    """
    Recursively get all files from the file share
    """
    all_files = []

    content = get_files_and_directories_in_share(share_client, directory_path)

    # Add files from current directory
    all_files.extend(content["files"])

    # Recursively process subdirectories
    for directory in content["directories"]:
        subdirectory_files = get_all_files_recursive(share_client, directory["path"])
        all_files.extend(subdirectory_files)

    return all_files


def print_file_shares(shares: List[Dict[str, Any]]) -> None:
    """Print file shares in a readable format"""
    print("File shares in storage account:")
    for i, share in enumerate(shares, 1):
        print(f"\t{i}. Name: {share['name']}")
        print(f"\tQuota: {share['quota']} GB")
        if share["metadata"]:
            print(f"\tMetadata: {share['metadata']}")
        else:
            print("\tMetadata: None")
        print(f"\tLast Modified: {share['last_modified']}")
        print()


def create_file_share_if_not_exists(
    share_service_client: ShareServiceClient, share_name: str, quota: int = 5120
) -> ShareClient:
    """
    Create a file share if it doesn't exist and return the share client
    """
    try:
        share_client = share_service_client.get_share_client(share_name)
        # Try to get share properties to check if it exists
        share_client.get_share_properties()
        print(f"✓ File share '{share_name}' already exists")
        return share_client
    except HttpResponseError as e:
        if e.status_code == 404:  # Share not found
            try:
                share_client = share_service_client.create_share(
                    share_name, quota=quota
                )
                print(f"✓ Created new file share '{share_name}' with {quota} GB quota")
                return share_client
            except HttpResponseError as create_error:
                print(f"Failed to create file share '{share_name}': {create_error}")
                raise
        else:
            print(f"Error checking file share '{share_name}': {e}")
            raise


def create_directory_if_not_exists(
    share_client: ShareClient, directory_path: str
) -> None:
    """
    Create directory structure if it doesn't exist
    """
    if not directory_path:
        return

    path_parts = directory_path.split("/")
    current_path = ""

    for part in path_parts:
        if current_path:
            current_path += f"/{part}"
        else:
            current_path = part

        try:
            directory_client = share_client.get_directory_client(current_path)
            directory_client.get_directory_properties()
        except HttpResponseError as e:
            if e.status_code == 404:
                try:
                    directory_client.create_directory()
                    print(f"✓ Created directory: {current_path}")
                except HttpResponseError as create_error:
                    print(
                        f"Failed to create directory '{current_path}': {create_error}"
                    )
                    raise


def check_file_access_permissions(file_client: ShareFileClient) -> bool:
    """
    Check if we have access to read from the source file
    """
    try:
        properties = file_client.get_file_properties()
        print(f"✓ Successfully accessed file: {file_client.file_name}")
        print(f"  - Size: {properties.size} bytes")
        print(f"  - Last modified: {properties.last_modified}")
        return True
    except HttpResponseError as e:
        print(f"Failed to access file: {file_client.file_name}")
        print(f"  - Error: {e}")
        return False


def check_share_permissions(share_client: ShareClient) -> bool:
    """
    Check if we have access to the file share
    """
    try:
        properties = share_client.get_share_properties()
        print(f"✓ Successfully accessed file share: {share_client.share_name}")
        return True
    except HttpResponseError as e:
        print(f"Failed to access file share: {share_client.share_name}")
        print(f"  - Error: {e}")
        return False


def print_files(files: List[Dict[str, Any]], share_name: str) -> None:
    """Print files in a readable format"""
    print(f"\nFiles in file share '{share_name}':")
    if files:
        for i, file_info in enumerate(files, 1):
            print(f"{i}. Name: {file_info['name']}")
            print(f"   Path: {file_info['path']}")
            print(f"   Size: {file_info['size']} bytes")
            print(f"   Last Modified: {file_info['last_modified']}")
            if file_info["metadata"]:
                print(f"   Metadata: {file_info['metadata']}")
            print()
    else:
        print("No files found in this file share")

    print(f"Total files: {len(files)}")


if __name__ == "__main__":
    storage_accounts: dict[str, str] = retrieve_storage_account()
    source_storage_account_url = storage_accounts["source"]
    destination_storage_account_url = storage_accounts["destination"]
    overwrite: bool = storage_accounts["overwrite"].lower() == "true"

    azure_auth: ClientSecretCredential = authenticate_azure()
    source_file_share_storage: ShareServiceClient = authenticate_file_share_storage(
        storage_account_url=source_storage_account_url, credential=azure_auth
    )
    destination_file_share_storage: ShareServiceClient = (
        authenticate_file_share_storage(
            storage_account_url=destination_storage_account_url, credential=azure_auth
        )
    )

    shares_list_source: List[Dict[str, Any]] = get_file_shares_in_storage(
        share_service_client=source_file_share_storage
    )
    shares_list_destination: List[Dict[str, Any]] = get_file_shares_in_storage(
        share_service_client=destination_file_share_storage
    )

    if len(shares_list_source) == 0:
        print("No file shares found in source storage account. Nothing to copy.")
        exit(1)

    if len(shares_list_destination) == 0:
        print("No file shares found in destination storage account.")

    for share in shares_list_source:
        share_name = share["name"]

        source_share_client: ShareClient = authenticate_file_share(
            share_service_client=source_file_share_storage, share_name=share_name
        )

        files_list_source: List[Dict[str, Any]] = get_all_files_recursive(
            source_share_client
        )

        if len(files_list_source) == 0:
            print(
                f"No files found in source file share '{share_name}'. Nothing to copy."
            )
            continue

        print(f"Source Storage Account: {source_storage_account_url}")
        print(f"Destination Storage Account: {destination_storage_account_url}")
        print(f"Current File Share Name: {share_name}")
        print(
            f"Number of file shares in source storage account: {len(shares_list_source)}"
        )
        print(
            f"Number of file shares in destination storage account: {len(shares_list_destination)}"
        )
        print(
            f"Number of files in source file share '{share_name}': {len(files_list_source)}"
        )

        # Create destination file share if it doesn't exist
        print(f"--- Checking/Creating destination file share '{share_name}' ---")
        dest_share_client = create_file_share_if_not_exists(
            destination_file_share_storage, share_name, quota=share.get("quota", 5120)
        )

        for file_info in files_list_source:
            file_path = file_info["path"]
            print(f"Found file: {file_path}")

            # Get the source file client
            source_file_client: ShareFileClient = source_share_client.get_file_client(
                file_path
            )

            # Create directory structure in destination if needed
            directory_path = (
                "/".join(file_path.split("/")[:-1]) if "/" in file_path else ""
            )
            if directory_path:
                create_directory_if_not_exists(dest_share_client, directory_path)

            # Get destination file client
            destination_file_client: ShareFileClient = (
                dest_share_client.get_file_client(file_path)
            )

            try:
                print(f"Starting copy of {file_path} to destination file share")
                copy_file_between_shares(
                    source_file_client, destination_file_client, overwrite
                )
                print(f"✓ Successfully copied: {file_path}")
            except HttpResponseError as e:
                print(f"Copy failed for {file_path} with error: {e}")
                print(f"Status code: {e.status_code}")
                continue
