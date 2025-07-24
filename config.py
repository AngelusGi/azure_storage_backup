"""
Configuration management for Azure Storage operations.

This module handles configuration loading, validation, and environment variable management
following Azure best practices for secure and maintainable configuration.
"""

import os
import logging
from typing import Optional
from dataclasses import dataclass
from enum import Enum

from dotenv import load_dotenv

from display import DisplayManager


class LogLevel(Enum):
    """Logging levels for the application."""
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL


@dataclass
class StorageConfig:
    """
    Configuration settings for Azure Storage operations.
    
    Attributes:
        storage_url: Azure Storage account URL
        tenant_id: Optional Azure tenant ID for authentication
        log_level: Logging level for the application
        container_name: Optional container name. If provided, only the specified 
                       container will be copied. If None, all containers will be copied.
    """
    storage_url: str
    tenant_id: Optional[str] = None
    log_level: LogLevel = LogLevel.INFO
    container_name: Optional[str] = None
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.storage_url:
            raise ValueError("Storage URL cannot be empty")
        # container_name is optional - if not provided, all containers will be processed
        if not self.container_name:
            print("ℹ️  Container name not provided - all containers will be processed")


class ConfigurationManager:
    """Manages application configuration and environment variables."""
    
    def __init__(self):
        """Initialize configuration manager and load environment variables."""
        self.display = DisplayManager()
        self._load_environment()
    
    def _load_environment(self) -> None:
        """Load environment variables from .env file."""
        if load_dotenv():
            self.display.print_info("Environment variables loaded from .env file")
        else:
            self.display.print_warning("Failed to load .env file, using system environment variables")
    
    def get_storage_config(self) -> StorageConfig:
        """Create and validate storage configuration from environment variables."""
        storage_url = os.getenv("STORAGE_ACCOUNT_URL_SOURCE")
        container_name = os.getenv("STORAGE_BLOB_NAME_SOURCE")
        tenant_id = os.getenv("AZURE_TENANT_ID")
        
        if not storage_url:
            raise ValueError(
                "Required environment variables missing. "
                "Please set STORAGE_ACCOUNT_URL_SOURCE."
            )
        
        if not container_name:
            self.display.print_info("No specific container configured - will process all containers")
        else:
            self.display.print_info(f"Configured to process container: {container_name}")
        
        return StorageConfig(
            storage_url=storage_url,
            container_name=container_name,
            tenant_id=tenant_id
        )
