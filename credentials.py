"""
Azure credential management with robust authentication strategies.

This module provides secure Azure authentication using ChainedTokenCredential
with proper fallback strategies, credential validation, and logging configuration.
"""

import sys
import os
import logging

from azure.identity import (
    ChainedTokenCredential, 
    EnvironmentCredential,
    ManagedIdentityCredential
)
from azure.core.exceptions import ClientAuthenticationError
from azure.core.credentials import TokenCredential

from config import StorageConfig
from display import DisplayManager


class CredentialManager:
    """Manages Azure authentication credentials with fallback strategies."""
    
    def __init__(self, config: StorageConfig, display: DisplayManager):
        """Initialize credential manager with configuration and display handler."""
        self.config = config
        self.display = display
        self._setup_logging()
    
    def _setup_logging(self) -> None:
        """Configure Azure SDK logging for debugging authentication issues."""
        loggers = ["azure.identity", "azure.core", "azure.storage"]
        
        for logger_name in loggers:
            logger = logging.getLogger(logger_name)
            logger.setLevel(self.config.log_level.value)
            
            if not logger.handlers:
                handler = logging.StreamHandler(sys.stdout)
                formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                handler.setFormatter(formatter)
                logger.addHandler(handler)
    
    def get_credential(self) -> TokenCredential:
        """
        Get Azure credential with robust fallback strategy.
        
        Returns:
            TokenCredential: Azure credential for authentication
            
        Raises:
            ClientAuthenticationError: If all authentication methods fail
        """
        return self._get_chained_credential()
    
    def _get_tenant_specific_credential(self) -> ChainedTokenCredential:
        """Get credential chain for a specific tenant."""
        if not self.config.tenant_id:
            raise ValueError("Tenant ID is required but not provided")
            
        self.display.print_info(f"Using tenant-specific credential for: {self.config.tenant_id}")
        
        # Create a custom credential chain with tenant-aware credentials
        return ChainedTokenCredential(
            # Environment credential with tenant ID
            EnvironmentCredential(),
            # Managed Identity for Azure-hosted scenarios
            ManagedIdentityCredential())
    
    def _get_chained_credential(self) -> ChainedTokenCredential:
        """Get credential using a custom chain for better control."""
        if self.config.tenant_id:
            self.display.print_info(f"Using tenant-specific credential chain for: {self.config.tenant_id}")
            # Ensure the tenant ID is set in environment for EnvironmentCredential
            os.environ["AZURE_TENANT_ID"] = self.config.tenant_id
            
            return ChainedTokenCredential(
                # Service principal via environment variables
                EnvironmentCredential(),
                # For Azure-hosted scenarios
                ManagedIdentityCredential())
        else:
            self.display.print_info("Using default credential chain")
            return ChainedTokenCredential(
                # Service principal via environment variables
                EnvironmentCredential(),
                # For Azure-hosted scenarios
                ManagedIdentityCredential())
    
    def validate_credential(self, credential: TokenCredential) -> bool:
        """
        Validate credential by attempting to get a token.
        
        Args:
            credential: The credential to validate
            
        Returns:
            bool: True if credential is valid, False otherwise
        """
        try:
            self.display.print_info("Validating Azure credential...")
            token = credential.get_token("https://storage.azure.com/.default")
            self.display.print_success(f"Credential validated. Token expires at: {token.expires_on}")
            return True
        except ClientAuthenticationError as e:
            self.display.print_error(f"Credential validation failed: {e}")
            return False
        except Exception as e:
            self.display.print_error(f"Unexpected error during credential validation: {e}")
            return False
