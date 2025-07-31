from azure.core.exceptions import ClientAuthenticationError, HttpResponseError
from azure.identity import ClientSecretCredential
from azure.storage.blob import (
    BlobClient,
    BlobServiceClient,
    ContainerClient,
    ContainerProperties,
    BlobProperties,
)
from typing import List
import logging
import os
from storage_manager import ItemPaged

logging.basicConfig(level=logging.WARNING)


def retrieve_storage_account() -> dict[str, str]:
    """
    Retrieve the storage account information from environment variables.
    """
    source = os.getenv("SOURCE_STORAGE_ACCOUNT_BLOB", None)
    destination = os.getenv("DESTINATION_STORAGE_ACCOUNT_BLOB", None)

    overwrite: str = os.getenv("OVERWRITE_STORAGE_ACCOUNT_BLOB", "False")

    if not all([source, destination]):
        raise ValueError(
            "Environment variables SOURCE_STORAGE_ACCOUNT_BLOB and DESTINATION_STORAGE_ACCOUNT_BLOB must be set."
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


def copy_from_source_in_azure(source_blob: BlobClient, destination_blob: BlobClient):
    # Get the source blob URL and create the destination blob
    # set overwrite param to True if you want to overwrite existing blob data
    destination_blob.upload_blob_from_url(source_url=source_blob.url, overwrite=False)


def authenticate_blob_storage(
    storage_account_url: str,
    credential: ClientSecretCredential | None,
) -> BlobServiceClient:
    """
    credential is an instance of ClientSecretCredential, or None.
    None if you want to use SAS token attached to storage_account_url.
    """
    return BlobServiceClient(account_url=storage_account_url, credential=credential)


def get_containers_in_storage(
    blob_service_client: BlobServiceClient,
) -> List[ContainerProperties]:
    """
    Returns a list of container objects from the storage account.
    Each object contains full container properties including name, metadata, etc.
    """
    containers_list: List[ContainerProperties] = []

    try:
        container_iterator: ItemPaged[ContainerProperties] = (
            blob_service_client.list_containers(include_metadata=True)
        )
        # Convert ItemPaged iterator to a standard list
        containers_list = list(container_iterator)
    except HttpResponseError as e:
        print(f"Error listing containers: {e}")
        raise

    if len(containers_list) == 0:
        print(
            f"No containers found in the storage account {blob_service_client.account_name}."
        )
    else:
        print_containers(containers_list)

    return containers_list


def authenticate_storage_container(
    blob_storage_client: BlobServiceClient, container: ContainerProperties | str
) -> ContainerClient:
    """
    Authenticate to a specific container in the storage account.
    This function assumes that the BlobServiceClient has already been created.
    """
    # Note: You need to provide the container name and BlobServiceClient instance
    # when calling this function in your application.
    return blob_storage_client.get_container_client(container)


def get_blobs_in_container(container_client: ContainerClient) -> List[BlobProperties]:
    """
    Returns a list of blob objects from the specified container.
    Each object contains full blob properties including name, size, last_modified, etc.
    """
    blob_list: List[BlobProperties] = []
    for blob in container_client.list_blobs():
        blob_list.append(blob)

    return blob_list


def print_containers(containers: List[ContainerProperties]) -> None:
    """Print containers in a readable format"""
    print("Containers in storage account:")
    for i, container in enumerate(containers, 1):
        print(f"\t{i}. Name: {container.name}")
        if container.metadata:
            print(f"\tMetadata: {container.metadata}")
        else:
            print("\tMetadata: None")


def create_container_if_not_exists(
    blob_service_client: BlobServiceClient, container_name: str
) -> ContainerClient:
    """
    Create a container if it doesn't exist and return the container client
    """
    try:
        container_client = blob_service_client.get_container_client(container_name)
        # Try to get container properties to check if it exists
        container_client.get_container_properties()
        print(f"✓ Container '{container_name}' already exists")
        return container_client
    except HttpResponseError as e:
        if e.status_code == 404:  # Container not found
            try:
                container_client = blob_service_client.create_container(container_name)
                print(f"✓ Created new container '{container_name}'")
                return container_client
            except HttpResponseError as create_error:
                print(f"Failed to create container '{container_name}': {create_error}")
                raise
        else:
            print(f"Error checking container '{container_name}': {e}")
            raise


def check_blob_access_permissions(blob_client: BlobClient) -> bool:
    """
    Check if we have access to read from the source blob
    """
    try:
        # Try to get blob properties (read permission test)
        properties = blob_client.get_blob_properties()
        print(f"✓ Successfully accessed blob: {blob_client.blob_name}")
        print(f"  - Size: {properties.size} bytes")
        print(f"  - Last modified: {properties.last_modified}")
        return True
    except HttpResponseError as e:
        print(f"Failed to access blob: {blob_client.blob_name}")
        print(f"  - Error: {e}")
        return False


def check_container_permissions(container_client: ContainerClient) -> bool:
    """
    Check if we have access to the container
    """
    try:
        # Try to get container properties
        properties: ContainerProperties = container_client.get_container_properties()
        print(f"✓ Successfully accessed container: {container_client.container_name}")
        return True
    except HttpResponseError as e:
        print(f"Failed to access container: {container_client.container_name}")
        print(f"  - Error: {e}")
        return False


def print_blobs(blobs: List[BlobProperties], container_name: str) -> None:
    """Print blobs in a readable format"""
    print(f"\nBlobs in container '{container_name}':")
    if blobs:
        for i, blob in enumerate(blobs, 1):
            print(f"{i}. Name: {blob.name}")
            print(f"Size: {blob.size} bytes")
            print(f"Last Modified: {blob.last_modified}")
            print(
                f"Content Type: {blob.content_settings.content_type if blob.content_settings else 'Unknown'}"
            )
            print()
    else:
        print("No blobs found in this container")

    print(f"Total blobs: {len(blobs)}")


if __name__ == "__main__":
    storage_accounts: dict[str, str] = retrieve_storage_account()
    source_storage_account_url = storage_accounts["source"]
    destination_storage_account_url = storage_accounts["destination"]
    overwrite: bool = bool(storage_accounts["overwrite"])

    azure_auth: ClientSecretCredential = authenticate_azure()
    source_blob_storage: BlobServiceClient = authenticate_blob_storage(
        storage_account_url=source_storage_account_url, credential=azure_auth
    )
    destination_blob_storage: BlobServiceClient = authenticate_blob_storage(
        storage_account_url=destination_storage_account_url, credential=azure_auth
    )

    container_list_source: List[ContainerProperties] = get_containers_in_storage(
        blob_service_client=source_blob_storage
    )
    container_list_destination: List[ContainerProperties] = get_containers_in_storage(
        blob_service_client=destination_blob_storage
    )

    if len(container_list_source) == 0:
        print("No containers found in source storage account. Nothing to copy.")
        exit(1)

    if len(container_list_destination) == 0:
        print("No containers found in destination storage account.")

    for container in container_list_source:
        source_blob_auth: ContainerClient = authenticate_storage_container(
            blob_storage_client=source_blob_storage, container=container
        )

        blob_list_source: List[BlobProperties] = get_blobs_in_container(
            source_blob_auth
        )

        if len(blob_list_source) == 0:
            print(
                f"No blobs found in source container '{container.name}'. Nothing to copy."
            )
            exit(1)

        print(f"Source Storage Account: {source_storage_account_url}")
        print(f"Destination Storage Account: {destination_storage_account_url}")
        print(f"Current Container Name: {container.name}")
        print(
            f"Number of containers in source storage account: {len(container_list_source)}"
        )
        print(
            f"Number of containers in destination storage account: {len(container_list_destination)}"
        )
        print(
            f"Number of blobs in source container '{container.name}': {len(blob_list_source)}"
        )

        if len(blob_list_source) == 0:
            print("No blobs found in source container. Nothing to copy.")
            exit(0)

        for blob in blob_list_source:
            print(f"Found blob: {blob.name}")

            # Get the source blob client properly
            source_blob_client: BlobClient = source_blob_storage.get_blob_client(
                container=container.name, blob=blob.name
            )

            # Use the proper blob URL from the client
            print(f"Source blob URL: {source_blob_client.url}")

            # Create destination container if it doesn't exist
            print(f"--- Checking/Creating destination container '{container.name}' ---")
            dest_container_client = create_container_if_not_exists(
                destination_blob_storage, container.name
            )

            destination_copy_blob: BlobClient = (
                destination_blob_storage.get_blob_client(
                    container=container.name, blob=blob.name
                )
            )

            try:
                print(
                    f"Starting copy of {source_blob_client.url} to {destination_copy_blob.url}"
                )
                copy_result = destination_copy_blob.upload_blob_from_url(
                    source_url=source_blob_client.url,
                    metadata=source_blob_client.get_blob_properties().metadata,
                    overwrite=overwrite,
                )
                # copy_result = destination_copy_blob.start_copy_from_url(source_url=copy_source_blob, metadata=sample_blob.metadata, requires_sync=True)
                print(f"Copy result: {copy_result}")
            except HttpResponseError as e:
                print(f"Copy failed with error: {e}")
                print(f"Status code: {e.status_code}")
                raise
