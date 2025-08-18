# Storage Account Backup Tool

## Tables

### Required enviromnet variables

```bash
export AZURE_TENANT_ID="YOUR-TENANT-ID"
export AZURE_CLIENT_ID="YOUR-SERVICE-PRINCIPAL-CLIENT-ID"
export AZURE_CLIENT_SECRET="YOUR-SERVICE-PRINCIPAL-CLIENT-SECRET"
export AZURE_SOURCE_STORAGE_ACCOUNT_TABLE="YOUR-AZURE-STORAGE-ACCOUNT-TABLE-NAME-OR-URL"
export AZURE_DESTINATION_STORAGE_ACCOUNT_TABLE="YOUR-AZURE-STORAGE-ACCOUNT-TABLE-NAME-OR-URL"
```

### Required Azure RBAC

- On **SOURCE** Storage Account `Storage Table Data Reader` permission must be assigned to service principal used to execute this script.
- On **DESTINATION** Storage Account `Storage Table Data Contributor` permission must be assigned to service principal used to execute this script.
