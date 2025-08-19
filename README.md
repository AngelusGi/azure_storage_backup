# Storage Account Backup Tool

## Azure Authentication

This module uses Azure Service Principal authentication to connect to Azure services. The authentication process is based on three required environment variables that must be set before using any Azure storage components (queue, table, blob, file share).

### Azure Authentication | Required Environment Variables

- `AZURE_TENANT_ID`: The tenant ID of your Azure Active Directory
- `AZURE_CLIENT_ID`: The client ID of your Azure Service Principal
- `AZURE_CLIENT_SECRET`: The client secret of your Azure Service Principal

### Azure Authentication | Authentication Process

1. The service principal credentials are automatically retrieved from the environment variables
2. Azure SDK uses these credentials to authenticate with Azure Active Directory
3. Upon successful authentication, an access token is obtained
4. This token is used to authorize requests to all Azure storage services (Queue, Table, Blob, File Share)

### Azure Authentication | Setup Instructions

Before running the application, ensure you have:

1. Created a Service Principal in Azure Active Directory
2. Granted appropriate permissions to the Service Principal for the storage resources
3. Exported the three environment variables with your actual values

The authentication is centralized and automatically handled by the Azure SDK across all storage service components.

## Blob Storage

### Blob Storage | Required environment variables

```bash
export AZURE_SOURCE_STORAGE_ACCOUNT_BLOB="YOUR-AZURE-STORAGE-ACCOUNT-BLOB-NAME-OR-URL"
export AZURE_DESTINATION_STORAGE_ACCOUNT_BLOB="YOUR-AZURE-STORAGE-ACCOUNT-BLOB-NAME-OR-URL"
```

### Blob Storage | Optional environment variables

```bash
export OVERWRITE_STORAGE_ACCOUNT_BLOB="true"  # Optional: Set to "true" to overwrite existing blobs.
```

### Blob Storage | Required Azure RBAC

- On **SOURCE** Storage Account `Storage Blob Data Reader` permission must be assigned to service principal used to execute this script.
- On **DESTINATION** Storage Account `Storage Blob Data Contributor` permission must be assigned to service principal used to execute this script.

### Blob Storage | Configuration Options

The `OVERWRITE_STORAGE_ACCOUNT_BLOB` environment variable controls the behavior when a blob already exists in the destination:

- `false` (default): Skip existing blobs without overwriting
- `true`: Overwrite existing blobs with source content

## Queue

### Queue | Required enviromnet variables

```bash
export AZURE_SOURCE_STORAGE_ACCOUNT_QUEUE="YOUR-AZURE-STORAGE-ACCOUNT-QUEUE-NAME-OR-URL"
export AZURE_DESTINATION_STORAGE_ACCOUNT_QUEUE="YOUR-AZURE-STORAGE-ACCOUNT-QUEUE-NAME-OR-URL"
```

### Queue | Required Azure RBAC

- On **SOURCE** Storage Account `Storage Queue Data Reader` and `Storage Queue Data Message Processor` permission must be assigned to service principal used to execute this script.
- On **DESTINATION** Storage Account `Storage Queue Data Contributor` and `Storage Queue Data Message Sender` permission must be assigned to service principal used to execute this script.

## Tables

### Tables | Required enviromnet variables

```bash
export AZURE_TENANT_ID="YOUR-TENANT-ID"
export AZURE_CLIENT_ID="YOUR-SERVICE-PRINCIPAL-CLIENT-ID"
export AZURE_CLIENT_SECRET="YOUR-SERVICE-PRINCIPAL-CLIENT-SECRET"
export AZURE_SOURCE_STORAGE_ACCOUNT_TABLE="YOUR-AZURE-STORAGE-ACCOUNT-TABLE-NAME-OR-URL"
export AZURE_DESTINATION_STORAGE_ACCOUNT_TABLE="YOUR-AZURE-STORAGE-ACCOUNT-TABLE-NAME-OR-URL"
```

### Tables | Required Azure RBAC

- On **SOURCE** Storage Account `Storage Table Data Reader` permission must be assigned to service principal used to execute this script.
- On **DESTINATION** Storage Account `Storage Table Data Contributor` permission must be assigned to service principal used to execute this script.

## Additional Notes

### Private Endpoint

When using Azure Storage with **private endpoints**, you must provide the **full FQDN** (Fully Qualified Domain Name) of the storage endpoint in the environment variables.

For **public access**, you have two options:

- Provide the **full endpoint URL** (e.g., `https://mystorageaccount.table.core.windows.net`)
- Provide **only the storage account name** (e.g., `mystorageaccount`) - the application will automatically construct the full URL

#### Examples

**Private Endpoint (FQDN required):**

```bash
export AZURE_SOURCE_STORAGE_ACCOUNT_BLOB="https://mystorageaccount.privatelink.blob.core.windows.net"
export AZURE_DESTINATION_STORAGE_ACCOUNT_BLOB="https://mystorageaccount.privatelink.blob.core.windows.net"
```

**Public Access (both formats supported):**

```bash
# Option 1: Full URL
export AZURE_SOURCE_STORAGE_ACCOUNT_BLOB="https://mystorageaccount.blob.core.windows.net"
export AZURE_DESTINATION_STORAGE_ACCOUNT_BLOB="https://mystorageaccount.blob.core.windows.net"

# Option 2: Storage account name only
export AZURE_SOURCE_STORAGE_ACCOUNT_BLOB="mystorageaccount"
export AZURE_DESTINATION_STORAGE_ACCOUNT_BLOB="mystorageaccount"
```

This applies to all storage services (Queue, Table, Blob, File Share) when configuring their respective environment variables.

This module uses Azure Service Principal authentication to connect to Azure services. The authentication process is based on three required environment variables that must be set before using any Azure storage components (queue, table, blob, file share).

### Azure Authentication | Required Environment Variables

- `AZURE_TENANT_ID`: The tenant ID of your Azure Active Directory
- `AZURE_CLIENT_ID`: The client ID of your Azure Service Principal
- `AZURE_CLIENT_SECRET`: The client secret of your Azure Service Principal

### Azure Authentication | Authentication Process

1. The service principal credentials are automatically retrieved from the environment variables
2. Azure SDK uses these credentials to authenticate with Azure Active Directory
3. Upon successful authentication, an access token is obtained
4. This token is used to authorize requests to all Azure storage services (Queue, Table, Blob, File Share)

### Azure Authentication | Setup Instructions

Before running the application, ensure you have:

1. Created a Service Principal in Azure Active Directory
2. Granted appropriate permissions to the Service Principal for the storage resources
3. Exported the three environment variables with your actual values

The authentication is centralized and automatically handled by the Azure SDK across all storage service components.

## Queue

### Queue | Required enviromnet variables

```bash
export AZURE_SOURCE_STORAGE_ACCOUNT_QUEUE="YOUR-AZURE-STORAGE-ACCOUNT-QUEUE-NAME-OR-URL"
export AZURE_DESTINATION_STORAGE_ACCOUNT_QUEUE="YOUR-AZURE-STORAGE-ACCOUNT-QUEUE-NAME-OR-URL"
```

### Queue | Required Azure RBAC

- On **SOURCE** Storage Account `Storage Queue Data Reader` and `Storage Queue Data Message Processor` permission must be assigned to service principal used to execute this script.
- On **DESTINATION** Storage Account `Storage Queue Data Contributor` and `Storage Queue Data Message Sender` permission must be assigned to service principal used to execute this script.

## Tables

### Tables | Required enviromnet variables

```bash
export AZURE_TENANT_ID="YOUR-TENANT-ID"
export AZURE_CLIENT_ID="YOUR-SERVICE-PRINCIPAL-CLIENT-ID"
export AZURE_CLIENT_SECRET="YOUR-SERVICE-PRINCIPAL-CLIENT-SECRET"
export AZURE_SOURCE_STORAGE_ACCOUNT_TABLE="YOUR-AZURE-STORAGE-ACCOUNT-TABLE-NAME-OR-URL"
export AZURE_DESTINATION_STORAGE_ACCOUNT_TABLE="YOUR-AZURE-STORAGE-ACCOUNT-TABLE-NAME-OR-URL"
```

### Tables | Required Azure RBAC

- On **SOURCE** Storage Account `Storage Table Data Reader` permission must be assigned to service principal used to execute this script.
- On **DESTINATION** Storage Account `Storage Table Data Contributor` permission must be assigned to service principal used to execute this script.

## Additional Notes

### Private Endpoint

When using Azure Storage with **private endpoints**, you must provide the **full FQDN** (Fully Qualified Domain Name) of the storage endpoint in the environment variables.

For **public access**, you have two options:

- Provide the **full endpoint URL** (e.g., `https://mystorageaccount.table.core.windows.net`)
- Provide **only the storage account name** (e.g., `mystorageaccount`) - the application will automatically construct the full URL

#### Examples

**Private Endpoint (FQDN required):**

```bash
export AZURE_SOURCE_STORAGE_ACCOUNT_TABLE="https://mystorageaccount.privatelink.table.core.windows.net"
export AZURE_DESTINATION_STORAGE_ACCOUNT_TABLE="https://mystorageaccount.privatelink.table.core.windows.net"
```

**Public Access (both formats supported):**

```bash
# Option 1: Full URL
export AZURE_SOURCE_STORAGE_ACCOUNT_TABLE="https://mystorageaccount.table.core.windows.net"
export AZURE_DESTINATION_STORAGE_ACCOUNT_TABLE="https://mystorageaccount.table.core.windows.net"

# Option 2: Storage account name only
export AZURE_SOURCE_STORAGE_ACCOUNT_TABLE="mystorageaccount"
export AZURE_DESTINATION_STORAGE_ACCOUNT_TABLE="mystorageaccount"
```

This applies to all storage services (Queue, Table, Blob, File Share) when configuring their respective environment variables.
