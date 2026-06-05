from azure.storage.blob import BlobServiceClient
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

CONN_STRING = os.environ["AZURE_CONNECTION_STRING"]
CONTAINER    = "bronze"
LOCAL_DIR = Path(__file__).parent.parent.parent / "data" / "bronze"

client = BlobServiceClient.from_connection_string(CONN_STRING)
container = client.get_container_client(CONTAINER)

for path in LOCAL_DIR.rglob("*.json"):
    parts = path.relative_to(LOCAL_DIR).parts
    
    if len(parts) == 4:
        entity, term, _, filename = parts
        blob_name = f"{entity}_{term}/{filename}"
    elif len(parts) == 3:
        entity, _, filename = parts
        blob_name = f"{entity}/{filename}"
    else:
        print(f"Skipping unexpected structure: {path}")
        continue

    with open(path, "rb") as f:
        container.upload_blob(blob_name, f, overwrite=False)