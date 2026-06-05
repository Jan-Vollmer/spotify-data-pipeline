from azure.storage.blob import BlobServiceClient
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

CONN_STRING = os.environ["AZURE_CONNECTION_STRING"]
CONTAINER    = "bronze"
ARTIST_DIR = Path(__file__).parent.parent.parent / "data" / "bronze" / "top_artists" / "short_term" / "archive"
TRACK_DIR = Path(__file__).parent.parent.parent / "data" / "bronze" / "top_tracks"

client = BlobServiceClient.from_connection_string(CONN_STRING)
container = client.get_container_client(CONTAINER)

for path in ARTIST_DIR.rglob("*.json"):
    blob_name = f"top_artists_short/{path.name}"
    
    print(f"Uploading: {blob_name}")
    with open(path, "rb") as f:
        container.upload_blob(blob_name, f, overwrite=True)
    print(f"Done: {blob_name}")

for path in TRACK_DIR.rglob("*.json"):
    parts = path.relative_to(TRACK_DIR).parts  # short_term/archive/file.json
    term, _, filename = parts
    term_short = term.replace("_term", "")
    blob_name = f"top_tracks_{term_short}/{filename}"
    
    print(f"Uploading: {blob_name}")
    with open(path, "rb") as f:
        container.upload_blob(blob_name, f, overwrite=True)
    print(f"Done: {blob_name}")    