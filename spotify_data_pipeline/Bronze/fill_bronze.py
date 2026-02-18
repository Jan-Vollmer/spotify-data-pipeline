import json
from pathlib import Path
from datetime import datetime
from itertools import islice
from .auth import get_or_refresh_token
from .get_top_tracks import get_top_tracks
from .get_top_artists import get_top_artists
from .get_recent_tracks import get_recent_tracks
from ..helpers.bronze_helper import write_bronze_batch, fetch_and_write
import logging
logging.basicConfig(level=logging.INFO)

def fill_bronze(limit_top: int = None, limit_recent: int = None):
    
    Path("data/bronze").mkdir(parents=True, exist_ok=True)
    
    downloaded_at = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

    access_top = get_or_refresh_token("user-top-read")
    access_recent = get_or_refresh_token("user-read-recently-played")
    
    fetch_and_write("top_tracks", get_top_tracks, access_top, downloaded_at, limit_top)
    fetch_and_write("top_artists", get_top_artists, access_top, downloaded_at, limit_top)
    recent_tracks = get_recent_tracks(access_recent, limit=limit_recent)
            
    write_bronze_batch(
    entity="recent_tracks",
    payload=recent_tracks,
    downloaded_at=downloaded_at
    )
    
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit_top", type=int, default=20)
    args = parser.parse_args()
    fill_bronze(limit_top=args.limit_top)