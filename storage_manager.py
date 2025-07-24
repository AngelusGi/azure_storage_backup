"""
Azure Blob Storage operations with proper error handling and resource cleanup.

This module provides a comprehensive interface for Azure Blob Storage operations
including container and blob listing, inventory display, and proper resource management.
"""

from typing import Optional, List
from contextlib import contextmanager

from azure.core.exceptions import ResourceNotFoundError, ServiceRequestError
from azure.core.paging import ItemPaged
from azure.storage.blob import BlobServiceClient, ContainerProperties, BlobProperties
from azure.core.credentials import TokenCredential

from config import StorageConfig
from display import DisplayManager


class StorageManager:
    """Manages Azure Blob Storage operations with proper error handling and resource cleanup."""
    
    def __init__(self, config: StorageConfig, credential: Optional[TokenCredential], display: DisplayManager):
        """Initialize storage manager with configuration, credential, and display handler."""
        self.config = config
        self.credential = credential
        self.display = display
        self._client: Optional[BlobServiceClient] = None
    
    @contextmanager
    def get_storage_client(self):
        """
        Context manager for BlobServiceClient with automatic cleanup.
        
        Yields:
            BlobServiceClient: Authenticated storage client
            
        Raises:
            ValueError: If credential is not set
            Exception: If client creation fails
        """
        if not self.credential:
            raise ValueError("Credential must be set before creating storage client")
            
        try:
            self._client = BlobServiceClient(
                account_url=self.config.storage_url,
                credential=self.credential
            )
            self.display.print_success("BlobServiceClient created successfully")
            self.display.print_info(f"Connected to storage account: {self._client.account_name}")
            yield self._client
        except Exception as e:
            self.display.print_error(f"Failed to create BlobServiceClient: {e}")
            raise
        finally:
            if self._client:
                self._client.close()
                self.display.print_success("Storage connection closed successfully")
    
    def list_containers(self, client: BlobServiceClient) -> List[ContainerProperties]:
        """
        List all containers in the storage account.
        
        Args:
            client: BlobServiceClient instance
            
        Returns:
            List[ContainerProperties]: List of container properties
            
        Raises:
            Exception: If listing containers fails
        """
        try:
            containers: ItemPaged[ContainerProperties] = client.list_containers(
                include_metadata=True,
                include_deleted=True,
                include_system=False
            )
            container_list = list(containers)
            self.display.print_info(f"Found {len(container_list)} containers")
            return container_list
        except ResourceNotFoundError:
            self.display.print_warning("Storage account not found or access denied")
            return []
        except ServiceRequestError as e:
            self.display.print_error(f"Service request failed: {e}")
            raise
        except Exception as e:
            self.display.print_error(f"Failed to list containers: {e}")
            raise
    
    def list_blobs_in_container(self, client: BlobServiceClient, container_name: str) -> List[BlobProperties]:
        """
        List all blobs in a specific container.
        
        Args:
            client: BlobServiceClient instance
            container_name: Name of the container
            
        Returns:
            List[BlobProperties]: List of blob properties
            
        Raises:
            Exception: If listing blobs fails
        """
        try:
            with client.get_container_client(container_name) as container_client:
                blobs: ItemPaged[BlobProperties] = container_client.list_blobs(
                    name_starts_with=None,
                    include=["metadata", "deleted", "snapshots"]
                )
                blob_list = list(blobs)
                self.display.print_info(f"Found {len(blob_list)} blobs in container '{container_name}'")
                return blob_list
        except ResourceNotFoundError:
            self.display.print_warning(f"Container '{container_name}' not found")
            return []
        except Exception as e:
            self.display.print_error(f"Failed to list blobs in container '{container_name}': {e}")
            raise
    
    def display_storage_inventory(self, client: BlobServiceClient) -> None:
        """
        Display a comprehensive inventory of containers and blobs.
        
        Args:
            client: BlobServiceClient instance
        """
        # If a specific container is configured, only process that one
        if self.config.container_name:
            self._display_single_container_inventory(client, self.config.container_name)
        else:
            self._display_all_containers_inventory(client)
    
    def _display_single_container_inventory(self, client: BlobServiceClient, container_name: str) -> None:
        """Display inventory for a single specified container."""
        self.display.print_info(f"Processing specific container: {container_name}")
        
        blobs = self.list_blobs_in_container(client, container_name)
        
        if not blobs:
            self.display.print_info(f"No blobs found in container '{container_name}'")
            return
        
        self.display.print_success(f"Container: {container_name} - {len(blobs)} blobs")
        for blob_idx, blob in enumerate(blobs, 1):
            self.display.print_info(
                f"  [{blob_idx}/{len(blobs)}] Blob: {blob.name} "
                f"(size: {blob.size} bytes, metadata: {blob.metadata}, deleted: {blob.deleted})"
            )
    
    def _display_all_containers_inventory(self, client: BlobServiceClient) -> None:
        """Display inventory for all containers in the storage account."""
        containers = self.list_containers(client)
        
        if not containers:
            self.display.print_warning("No containers found in storage account")
            return
        
        for idx, container in enumerate(containers, 1):
            self.display.print_success(
                f"[{idx}/{len(containers)}] Container: {container.name} "
                f"(metadata: {container.metadata}, deleted: {container.deleted})"
            )
            
            blobs = self.list_blobs_in_container(client, container.name)
            
            if not blobs:
                self.display.print_info(f"  No blobs found in container '{container.name}'")
                continue
            
            for blob_idx, blob in enumerate(blobs, 1):
                self.display.print_info(
                    f"  [{blob_idx}/{len(blobs)}] Blob: {blob.name} "
                    f"(size: {blob.size} bytes, metadata: {blob.metadata}, deleted: {blob.deleted})"
                )
