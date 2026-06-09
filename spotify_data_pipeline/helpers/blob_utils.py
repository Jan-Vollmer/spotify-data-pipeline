import io
import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

def get_blob_service_client() -> BlobServiceClient:
    return BlobServiceClient.from_connection_string(os.environ["AZURE_CONNECTION_STRING"])

def list_blobs(container: str, prefix: str) -> list[str]:
    client = get_blob_service_client()
    container_client = client.get_container_client(container)
    return [b.name for b in container_client.list_blobs(name_starts_with=prefix)]

def download_json_blob(container: str, blob_path: str) -> bytes:
    client = get_blob_service_client()
    return client.get_blob_client(container=container, blob=blob_path).download_blob().readall()

def move_blob_to_archive(container: str, blob_path: str):
    client = get_blob_service_client()
    parts = blob_path.split("/")
    archive_path = "/".join(parts[:-1] + ["archive", parts[-1]])
    src_url = client.get_blob_client(container=container, blob=blob_path).url
    dest = client.get_blob_client(container=container, blob=archive_path)
    dest.start_copy_from_url(src_url)
    client.get_blob_client(container=container, blob=blob_path).delete_blob()

def download_parquet_from_blob(container: str, blob_path: str) -> pd.DataFrame:
    client = get_blob_service_client()
    blob_client = client.get_blob_client(container=container, blob=blob_path)
    try:
        data = blob_client.download_blob().readall()
        return pd.read_parquet(io.BytesIO(data))
    except Exception:
        return pd.DataFrame()

def upload_parquet_to_blob(df: pd.DataFrame, container: str, blob_path: str):
    client = get_blob_service_client()
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    client.get_blob_client(container=container, blob=blob_path).upload_blob(buffer, overwrite=True)