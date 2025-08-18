import os
from azure.identity import ClientSecretCredential
from azure.data.tables import TableServiceClient, TableClient

### CONFIGURATION
TENANT_ID = os.getenv("AZURE_TENANT_ID", None)
CLIENT_ID = os.getenv("AZURE_CLIENT_ID", None)
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET", None)

SOURCE_ACCOUNT = os.getenv("AZURE_SOURCE_TABLE_STORAGE_ACCOUNT", None)
DEST_ACCOUNT = os.getenv("AZURE_DESTINATION_TABLE_STORAGE_ACCOUNT", None)

### Ensure all required environment variables are set
if not all([TENANT_ID, CLIENT_ID, CLIENT_SECRET]):
    print(
        "[!] Error: all authentication related environment variables must be set AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET."
    )
    exit(1)


if not all([SOURCE_ACCOUNT, DEST_ACCOUNT]):
    print(
        "[!] Error: all storage account related environment variables must be set AZURE_SOURCE_TABLE_STORAGE_ACCOUNT, AZURE_DESTINATION_TABLE_STORAGE_ACCOUNT."
    )
    exit(1)

### AUTHENTICATION
### Authenticate using a Service Principal (Client ID, Tenant ID, Client Secret)
credential = ClientSecretCredential(
    tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET
)

### TABLE STORAGE ENDPOINTS
### Define the endpoints for source and destination storage accounts
source_url = f"https://{SOURCE_ACCOUNT}.table.core.windows.net"
dest_url = f"https://{DEST_ACCOUNT}.table.core.windows.net"

### CREATE SERVICE CLIENTS
### TableServiceClient allows interaction with tables (create, list, delete, etc.)
source_service = TableServiceClient(endpoint=source_url, credential=credential)
dest_service = TableServiceClient(endpoint=dest_url, credential=credential)

### GET ALL SOURCE TABLES
print(f"[i] Retrieving list of tables from {SOURCE_ACCOUNT}...")
source_tables = source_service.list_tables()

### Loop through each table in the source account
for table in source_tables:
    table_name = table.name
    print(f"\n[i] Replicating table: {table_name}")

    ### Create clients for source and destination tables
    source_table_client: TableClient = source_service.get_table_client(
        table_name=table_name
    )
    dest_table_client: TableClient = dest_service.get_table_client(
        table_name=table_name
    )

    ### Create the table in the destination storage account if it does not exist
    try:
        dest_service.create_table(table_name)
        print(f"[+] Table {table_name} created in storage {DEST_ACCOUNT}")
    except Exception as e:
        print(f"[x] Could not create table {table_name} in storage {DEST_ACCOUNT}: {e}")

    ### Copy all entities from source table to destination table
    entities = list(source_table_client.list_entities())  ### materialize iterator
    total = len(entities)
    copied = 0

    for idx, entity in enumerate(entities, start=1):
        try:
            ### upsert_entity ensures insert or update if entity already exists
            dest_table_client.upsert_entity(entity)
            copied += 1
            print(f"\t[>] Copied {idx}/{total} entities...", end="\r")
        except Exception as e:
            print(f"[x] Error copying entity {entity.get('RowKey')}: {e}")

    print(f"\n[✓] Finished: {copied}/{total} entities copied into table {table_name}")

print("\n[i] Script completed.")
