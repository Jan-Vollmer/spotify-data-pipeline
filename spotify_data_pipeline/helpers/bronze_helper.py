import json
from typing import Any
import logging
import os
from azure.storage.blob import BlobServiceClient
from spotify_data_pipeline.helpers.deprecation import deprecated

AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
AZURE_CONTAINER = os.getenv("AZURE_CONTAINER")

def write_bronze_batch(
    entity: str,
    payload: Any,
    downloaded_at: str,
    subdir: str | None = None,
) -> str:
    logging.warning(f"AZURE_CONTAINER = {AZURE_CONTAINER}")
    logging.warning(f"AZURE_CONNECTION_STRING present = {bool(AZURE_CONNECTION_STRING)}")
    if subdir:
        blob_name = f"bronze/{entity}/{subdir}/{entity}_{downloaded_at}.json"
    else:
        blob_name = f"bronze/{entity}/{entity}_{downloaded_at}.json"

    data = json.dumps(payload, ensure_ascii=False, indent=4)

    blob_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING).get_blob_client(
    container=AZURE_CONTAINER, blob=blob_name
    )
    try:
        blob_client.upload_blob(data, overwrite=True)
        logging.warning(f"Uploaded blob: {blob_name}") 
    except Exception:
        logging.exception("Blob upload failed")
        raise
    return blob_name

@deprecated
def fetch_and_write(entity: str, getter_func, access_token: str, downloaded_at: str, limit: int = None, time_ranges: list[str] = None):
    if time_ranges is None:
        time_ranges = ["short_term", "medium_term", "long_term"]
    
    for tr in time_ranges:
        items = getter_func(access_token, limit=limit, time_range=tr)
        if items is None:
            raise ValueError(f"{entity} returned None")
        if not isinstance(items, (list, dict)):
            raise TypeError(f"{entity} unexpected type {type(items)}")
        if isinstance(items, (list, dict)) and len(items) == 0:
            logging.warning(f"{entity} returned empty payload for {tr}")
        logging.info(f"Writing {len(items)} items to {entity}/{tr}")    
        write_bronze_batch(
            entity=entity,
            payload=items,
            downloaded_at=downloaded_at,
            subdir=f"{tr}"
        )