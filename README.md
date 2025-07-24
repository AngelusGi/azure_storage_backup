# Azure Storage Client - Modular Architecture

This Azure Storage client has been refactored to follow Python best practices with a modular, maintainable architecture.

## 🏗️ Architecture Overview

The original monolithic `storage.py` file has been split into focused modules following the **Single Responsibility Principle**:

```bash
azure/client/
├── __init__.py          # Package exports and API
├── app.py               # Main application orchestrator
├── config.py            # Configuration management
├── credentials.py       # Azure authentication
├── storage_manager.py   # Blob storage operations
├── display.py           # Console output formatting
└── storage.py          # Legacy entry point (backward compatibility)
```

## 📦 Module Responsibilities

### `config.py` - Configuration Management

- **StorageConfig**: Dataclass for storage settings
- **ConfigurationManager**: Environment variable loading and validation
- **LogLevel**: Logging level enumeration

```python
from azure.client.config import StorageConfig, ConfigurationManager

config_manager = ConfigurationManager()
config = config_manager.get_storage_config()
```

### `credentials.py` - Authentication

- **CredentialManager**: Azure authentication with fallback strategies
- Implements ChainedTokenCredential with proper error handling
- Supports both tenant-specific and default authentication

```python
from azure.client.credentials import CredentialManager

cred_manager = CredentialManager(config, display)
credential = cred_manager.get_credential()
```

### `storage_manager.py` - Blob Storage Operations

- **StorageManager**: Azure Blob Storage operations
- Context manager for proper resource cleanup
- Supports both single-container and multi-container processing
- Comprehensive error handling and retry logic

```python
from azure.client.storage_manager import StorageManager

storage_manager = StorageManager(config, credential, display)
with storage_manager.get_storage_client() as client:
    storage_manager.display_storage_inventory(client)
```

### `display.py` - Output Formatting

- **DisplayManager**: Centralized console output with colors and icons
- Consistent formatting across all modules
- Cross-platform color support with colorama

```python
from azure.client.display import DisplayManager

display = DisplayManager()
display.print_success("Operation completed!")
display.print_error("Something went wrong")
```

### `app.py` - Application Orchestrator

- **AzureStorageApplication**: Main application class
- Coordinates all components and manages workflow
- Entry point with proper error handling

```python
from azure.client.app import AzureStorageApplication, main

# Programmatic usage
app = AzureStorageApplication()
app.run()

# Direct execution
main()
```

## 🚀 Usage Examples

### Basic Usage

```python
from azure.client.app import main
main()
```

## 🔧 Configuration

### Environment Variables

```bash
# Required
STORAGE_ACCOUNT_URL_SOURCE=https://yourstorageaccount.blob.core.windows.net

# Optional - specific container (if not set, all containers will be processed)
STORAGE_BLOB_NAME_SOURCE=container-name

# Optional - Azure tenant ID
AZURE_TENANT_ID=your-tenant-id

# Optional - Service Principal credentials
AZURE_CLIENT_ID=your-client-id
AZURE_CLIENT_SECRET=your-client-secret
```

### Container Processing Behavior

- **With `STORAGE_BLOB_NAME_SOURCE`**: Only the specified container is processed
- **Without `STORAGE_BLOB_NAME_SOURCE`**: All containers in the storage account are processed

## 🧪 Testing

Each module can be tested independently:

```python
# Test configuration
from azure.client.config import StorageConfig
config = StorageConfig(storage_url="https://test.blob.core.windows.net")

# Test display
from azure.client.display import DisplayManager
display = DisplayManager()
display.print_info("Test message")

# Test credential validation
from azure.client.credentials import CredentialManager
cred_manager = CredentialManager(config, display)
# ... test authentication logic
```

## 🔮 Future Enhancements

The modular architecture makes it easy to add:

- Additional authentication methods
- New storage operations (copy, sync, backup)
- Different output formats (JSON, CSV)
- Enhanced error recovery strategies
- Performance monitoring and metrics
- Support for other Azure storage services

This refactored architecture follows Python best practices and Azure SDK guidelines, making the codebase more maintainable, testable, and extensible.
