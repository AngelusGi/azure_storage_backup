import os, sys
from azure.identity import ClientSecretCredential
from azure.data.tables import TableServiceClient, TableClient

### CONFIGURATION
TENANT_ID = os.getenv("AZURE_TENANT_ID", None)
CLIENT_ID = os.getenv("AZURE_CLIENT_ID", None)
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET", None)

### Minimum required permission on Azure on AZURE_SOURCE_STORAGE_ACCOUNT_TABLE is 'Storage Table Data Reader' assigned to CLIENT_ID of Service Principal assigned at Storage Account level
SOURCE_ACCOUNT = os.getenv("AZURE_SOURCE_STORAGE_ACCOUNT_TABLE", None)
### Minimum required permission on Azure on AZURE_DESTINATION_STORAGE_ACCOUNT_TABLE is 'Storage Table Data Contributor' assigned to CLIENT_ID of Service Principal assigned at Storage Account level
DEST_ACCOUNT = os.getenv("AZURE_DESTINATION_STORAGE_ACCOUNT_TABLE", None)


def enforce_storage_table_url(url: str) -> str:
    if not url.endswith(".table.core.windows.net"):
        url = f"{url}.table.core.windows.net"
    if not url.startswith("https://"):
        url = f"https://{url}"
    return url


### Ensure all required environment variables are set
if not all([TENANT_ID, CLIENT_ID, CLIENT_SECRET]):
    raise ValueError(
        "[!] Error: all authentication related environment variables must be set AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET."
    )

if not all([SOURCE_ACCOUNT, DEST_ACCOUNT]):
    raise ValueError(
        "[!] Error: all storage account related environment variables must be set AZURE_SOURCE_STORAGE_ACCOUNT_TABLE, AZURE_DESTINATION_STORAGE_ACCOUNT_TABLE."
    )

### TABLE STORAGE ENDPOINTS
### Define the endpoints for source and destination storage accounts
source_url = enforce_storage_table_url(SOURCE_ACCOUNT)
print(f"[i] Source Table Storage URL: {source_url}")
dest_url = enforce_storage_table_url(DEST_ACCOUNT)
print(f"[i] Destination Table Storage URL: '{dest_url}'")

### AUTHENTICATION
### Authenticate using a Service Principal (Client ID, Tenant ID, Client Secret)
try:
    credential = ClientSecretCredential(
        tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET
    )
except Exception as e:
    print(f"[!] Unable to authenticate: {e}")
    sys.exit(1)

### CREATE SERVICE CLIENTS
### TableServiceClient allows interaction with tables (create, list, delete, etc.)
try:
    source_service = TableServiceClient(endpoint=source_url, credential=credential)
    dest_service = TableServiceClient(endpoint=dest_url, credential=credential)
except Exception as e:
    print(f"[x] Fatal error. Unable to create table service clients: {e}")
    sys.exit(1)

try:
    ### GET ALL SOURCE TABLES
    print(f"[i] Retrieving list of tables from {source_url}...")
    source_tables = source_service.list_tables()
except Exception as e:
    print(f"[x] Fatal error. Error retrieving tables from {source_url}: {e}")
    sys.exit(1)

### Loop through each table in the source account
for table in source_tables:
    table_name = table.name
    print(f"\n[i] Replicating table: '{table_name}'")

    try:
        ### Create clients for source and destination tables
        source_table_client: TableClient = source_service.get_table_client(
            table_name=table_name
        )
        dest_table_client: TableClient = dest_service.get_table_client(
            table_name=table_name
        )
    except Exception as e:
        print(f"[x] Error creating table clients for '{table_name}': {e}")
        sys.exit(1)

    ### Create the table in the destination storage account if it does not exist
    try:
        dest_service.create_table(table_name)
        print(f"[+] Table '{table_name}' created in storage '{dest_url}'")
    except Exception as e:
        print(f"[!] Could not create table '{table_name}' in storage '{dest_url}': {e}")

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
            print(f"[!] Error copying entity {entity.get('RowKey')}: {e}")

    print(f"\n[✓] Finished: {copied}/{total} entities copied into table '{table_name}'")

print("\n[i] Script completed.")
