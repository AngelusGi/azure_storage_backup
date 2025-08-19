import os
import subprocess
from azure.identity import ClientSecretCredential

# === CONFIGURAZIONE ===
TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
SUBSCRIPTION_ID = os.getenv("AZURE_SUBSCRIPTION_ID")

SOURCE_RESOURCE_GROUP = "source-rg"
SOURCE_STORAGE_ACCOUNT = "sourcestorageacct"
DEST_RESOURCE_GROUP = "dest-rg"
DEST_STORAGE_ACCOUNT = "deststorageacct"

# === AUTENTICAZIONE ===
credential = ClientSecretCredential(
    tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET
)
storage_client = StorageManagementClient(credential, SUBSCRIPTION_ID)

# === RECUPERA LE FILE SHARE SORGENTI ===
shares = storage_client.file_shares.list(
    resource_group_name=SOURCE_RESOURCE_GROUP, account_name=SOURCE_STORAGE_ACCOUNT
)


# === FUNZIONE PER COPIARE FILE SHARE ===
def copy_share(share_name):
    print(f"[+] Copia della share: {share_name}")

    # Costruzione degli URL
    source_url = f"https://{SOURCE_STORAGE_ACCOUNT}.file.core.windows.net/{share_name}"
    dest_url = f"https://{DEST_STORAGE_ACCOUNT}.file.core.windows.net/{share_name}"

    # Autenticazione con SP per azcopy
    subprocess.run(
        [
            "azcopy",
            "login",
            "--service-principal",
            "--application-id",
            CLIENT_ID,
            "--tenant-id",
            TENANT_ID,
            "--client-secret",
            CLIENT_SECRET,
        ],
        check=True,
    )

    # Copia share con azcopy
    subprocess.run(
        ["azcopy", "copy", f"{source_url}", f"{dest_url}", "--recursive"], check=True
    )


# === CICLO SU TUTTE LE SHARE ===
for share in shares:
    copy_share(share.name)

print("[✓] Copia completata.")
