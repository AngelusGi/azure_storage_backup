# Storage Account Backup Tool

## Table of Contents

1. [Azure Authentication and Configuration](#azure-authentication-and-configuration)
    - [Required Authentication Variables/Arguments](#required-authentication-variablesarguments)
    - [Usage: Environment Variables](#usage-environment-variables)
    - [Usage: CLI Arguments](#usage-cli-arguments)
    - [List of Supported CLI Arguments](#list-of-supported-cli-arguments)
    - [Example: Mixed Usage](#example-mixed-usage)
    - [Authentication Process](#authentication-process)
    - [Setup Instructions](#setup-instructions)

2. [Blob Storage](#blob-storage)
    - [Required environment variables](#blob-storage--required-environment-variables)
    - [Optional environment variables](#blob-storage--optional-environment-variables)
    - [Required Azure RBAC](#blob-storage--required-azure-rbac)
    - [Configuration Options](#blob-storage--configuration-options)

3. [Queue](#queue)
    - [Required environment variables](#queue--required-enviromnet-variables)
    - [Required Azure RBAC](#queue--required-azure-rbac)

4. [Tables](#tables)
    - [Required environment variables](#tables--required-enviromnet-variables)
    - [Required Azure RBAC](#tables--required-azure-rbac)

5. [File Share](#file-share)
    - [Required environment variables](#file-share--required-environment-variables)
    - [Authentication Process](#file-share--authentication-process)
    - [Required Permissions](#file-share--required-permissions)
    - [Usage Example](#file-share--usage-example)

6. [Additional Notes](#additional-notes)
    - [Private Endpoint](#private-endpoint)
    - [Examples](#examples)

## Azure Authentication and Configuration

This tool supports configuration via **environment variables** and/or **CLI arguments**. You can use either method, or mix them: CLI arguments take precedence over environment variables.

### Required Authentication Variables/Arguments

- `ARM_TENANT_ID` or `--tenant-id`: The tenant ID of your Azure Active Directory
- `ARM_CLIENT_ID` or `--client-id`: The client ID of your Azure Service Principal
- `ARM_CLIENT_SECRET` or `--client-secret`: The client secret of your Azure Service Principal

### Usage: Environment Variables

Set the required variables before running:

```bash
export ARM_TENANT_ID="your-tenant-id"
export ARM_CLIENT_ID="your-client-id"
export ARM_CLIENT_SECRET="your-client-secret"
export AZURE_SOURCE_STORAGE_ACCOUNT_BLOB="..."
export AZURE_DESTINATION_STORAGE_ACCOUNT_BLOB="..."
# ...other variables as documented below
python main.py
```

### Usage: CLI Arguments

You can pass any variable as a CLI argument. Example:

```bash
python main.py --tenant-id "your-tenant-id" --client-id "your-client-id" --client-secret "your-client-secret" --source-account-blob "..." --dest-account-blob "..."
```

If both CLI argument and environment variable are provided, the CLI argument is used.

### List of Supported CLI Arguments

- `--tenant-id`
- `--client-id`
- `--client-secret`
- `--source-account-blob`
- `--dest-account-blob`
- `--overwrite-blob`
- `--source-account-queue`
- `--dest-account-queue`
- `--source-account-table`
- `--dest-account-table`
- `--source-connection-string-file-share`
- `--dest-connection-string-file-share`

### Example: Mixed Usage

You can mix environment variables and CLI arguments:

```bash
export ARM_TENANT_ID="your-tenant-id"
python main.py --client-id "your-client-id" --client-secret "your-client-secret"
```

### Authentication Process

1. The service principal credentials are retrieved from CLI arguments or environment variables.
2. Azure SDK uses these credentials to authenticate with Azure Active Directory.
3. Upon successful authentication, an access token is obtained.
4. This token is used to authorize requests to all Azure storage services (Queue, Table, Blob, File Share).

### Setup Instructions

Before running the application, ensure you have:

1. Created a Service Principal in Azure Active Directory
2. Granted appropriate permissions to the Service Principal for the storage resources
3. Provided the required configuration via environment variables and/or CLI arguments

The authentication is centralized and automatically handled by the Azure SDK across all storage service components.

## Blob Storage

### Blob Storage | Required environment variables

```bash
export AZURE_SOURCE_STORAGE_ACCOUNT_BLOB="YOUR-AZURE-STORAGE-ACCOUNT-BLOB-NAME-OR-URL"
export AZURE_DESTINATION_STORAGE_ACCOUNT_BLOB="YOUR-AZURE-STORAGE-ACCOUNT-BLOB-NAME-OR-URL"
```

### Blob Storage | Optional environment variables

```bash
export OVERWRITE_STORAGE_ACCOUNT_BLOB="true"
```

### Blob Storage | Required Azure RBAC

- On **SOURCE** Storage Account `Storage Blob Data Reader` permission must be assigned to service principal used to execute this script.
- On **DESTINATION** Storage Account `Storage Blob Data Contributor` permission must be assigned to service principal used to execute this script.

### Blob Storage | Configuration Options

The `OVERWRITE_STORAGE_ACCOUNT_BLOB` environment variable controls the behavior when a blob already exists in the destination:

- `false` **(default)**: Skip existing blobs without overwriting
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
export AZURE_SOURCE_STORAGE_ACCOUNT_TABLE="YOUR-AZURE-STORAGE-ACCOUNT-TABLE-NAME-OR-URL"
export AZURE_DESTINATION_STORAGE_ACCOUNT_TABLE="YOUR-AZURE-STORAGE-ACCOUNT-TABLE-NAME-OR-URL"
```

### Tables | Required Azure RBAC

- On **SOURCE** Storage Account `Storage Table Data Reader` permission must be assigned to service principal used to execute this script.
- On **DESTINATION** Storage Account `Storage Table Data Contributor` permission must be assigned to service principal used to execute this script.

## File Share

### File Share | Required environment variables

```bash
export AZURE_SOURCE_CONNECTION_STRING_FILE_SHARE="<source-connection-string-with-SAS>"
export AZURE_DEST_CONNECTION_STRING_FILE_SHARE="<destination-connection-string-with-SAS>"
```

### File Share | Authentication Process

- Authentication is performed through a **connection string** containing the File endpoint and the `SharedAccessSignature` parameter.
- The **SAS token** is provided by the connection string for AzCopy and SDK operations.
- A Service Principal or Azure AD is not required in this mode: permissions are defined by the SAS.

### File Share | Required Permissions

- The SAS token must include the necessary permissions (`sp=rwdlc` for read, write, delete, list, create) and must be valid for the intended usage period.
- The `ss=f` parameter must be present to enable access to File Shares.

### File Share | Usage Example

```bash
export AZURE_SOURCE_CONNECTION_STRING_FILE_SHARE="FileEndpoint=https://<sourceaccount>.file.core.windows.net/;SharedAccessSignature=sv=..."
export AZURE_DEST_CONNECTION_STRING_FILE_SHARE="FileEndpoint=https://<destaccount>.file.core.windows.net/;SharedAccessSignature=sv=..."
```

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
