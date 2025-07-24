"""
Main Azure Storage Application orchestrator.

This module provides the main application class that coordinates all components
and manages the overall workflow for Azure Storage operations.
"""

import sys

from config import ConfigurationManager
from credentials import CredentialManager
from storage_manager import StorageManager
from display import DisplayManager


class AzureStorageApplication:
    """Main application class that orchestrates Azure Storage operations."""
    
    def __init__(self):
        """Initialize the application with all necessary components."""
        self.display = DisplayManager()
        self.config_manager = ConfigurationManager()
        
        try:
            self.config = self.config_manager.get_storage_config()
            self.credential_manager = CredentialManager(self.config, self.display)
            self.storage_manager = StorageManager(self.config, None, self.display)
        except Exception as e:
            self.display.print_error(f"Failed to initialize application: {e}")
            raise
    
    def run(self) -> None:
        """Execute the main application workflow."""
        try:
            # Get and validate credentials
            credential = self.credential_manager.get_credential()
            
            if not self.credential_manager.validate_credential(credential):
                self.display.print_error("Credential validation failed. Please check your authentication setup.")
                return
            
            # Update storage manager with validated credential
            self.storage_manager.credential = credential
            
            # Perform storage operations
            with self.storage_manager.get_storage_client() as storage_client:
                self.storage_manager.display_storage_inventory(storage_client)
                
        except KeyboardInterrupt:
            self.display.print_warning("Operation cancelled by user")
        except Exception as e:
            self.display.print_error(f"Application error: {e}")
            raise


def main() -> None:
    """Entry point for the application."""
    try:
        app = AzureStorageApplication()
        app.run()
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
