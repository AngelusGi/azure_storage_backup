import os
from azure.identity import ClientSecretCredential
from azure.data.tables import TableServiceClient, TableClient

# === CONFIGURAZIONE ===
TENANT_ID = os.getenv("AZURE_TENANT_ID", None)
CLIENT_ID = os.getenv("AZURE_CLIENT_ID", None)
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET", None)

SOURCE_ACCOUNT = "<your_source_account>"
DEST_ACCOUNT = "<your_dest_account>"

SOURCE_TABLE = "<your_source_table>"
DEST_TABLE = "<your_dest_table>"

if not all([TENANT_ID, CLIENT_ID, CLIENT_SECRET]):
    print("[!] Errore: tutte le variabili di ambiente devono essere impostate TENANT_ID, CLIENT_ID, CLIENT_SECRET.")
    exit(1)

# === AUTENTICAZIONE ===
credential = ClientSecretCredential(
    tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET
)

# === ENDPOINT TABLE STORAGE ===
source_url = f"https://{SOURCE_ACCOUNT}.table.core.windows.net"
dest_url = f"https://{DEST_ACCOUNT}.table.core.windows.net"

# === CLIENT PER LE TABELLE ===
source_service = TableServiceClient(endpoint=source_url, credential=credential)
dest_service = TableServiceClient(endpoint=dest_url, credential=credential)

# Recupera riferimenti alle tabelle
source_table_client: TableClient = source_service.get_table_client(
    table_name=SOURCE_TABLE
)
dest_table_client: TableClient = dest_service.get_table_client(table_name=DEST_TABLE)

# Crea la tabella di destinazione se non esiste
try:
    dest_service.create_table(DEST_TABLE)
    print(f"[+] Tabella {DEST_TABLE} creata nello storage {DEST_ACCOUNT}")
except Exception as e:
    print(f"[!] Errore nella creazione della tabella {DEST_TABLE}: {e}")
    exit(1)

# === COPIA DELLE RIGHE ===
print(f"[+] Copia dei dati da {SOURCE_TABLE} a {DEST_TABLE}...")

entities = source_table_client.list_entities()

count = 0
for entity in entities:
    try:
        dest_table_client.upsert_entity(entity)
        count += 1
    except Exception as e:
        print(f"[!] Errore copiando entità {entity.get('RowKey')}: {e}")

print(f"[=] Copia completata: {count} entità replicate.")
