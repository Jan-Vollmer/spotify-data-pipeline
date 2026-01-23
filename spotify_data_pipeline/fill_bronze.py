import json
from pathlib import Path
from datetime import datetime
from itertools import islice
from .auth import get_or_refresh_token
from .get_top_tracks import get_top_tracks
from .get_top_artists import get_top_artists
from .get_recent_tracks import get_recent_tracks
from .helpers.bronze_helper import write_bronze_batch, fetch_and_write

def fill_bronze(limit_top: int = None, limit_recent: int = None):
    
    Path("data/bronze").mkdir(parents=True, exist_ok=True)
    
    downloaded_at = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

    access_top = get_or_refresh_token("user-top-read")
    access_recent = get_or_refresh_token("user-read-recently-played")
    
    fetch_and_write("top_tracks", get_top_tracks, access_top, downloaded_at, limit_top)
    fetch_and_write("top_artists", get_top_artists, access_top, downloaded_at, limit_top)
    recent_tracks = get_recent_tracks(access_recent, limit=limit_recent)
        
    print("\nZuletzt gespielt:")
    for i, r in enumerate(islice(recent_tracks, 10), start=1):
        track = r["track"]
        print(f"{i}. {track['name']} – {track['artists'][0]['name']}")
    
    
    write_bronze_batch(
    entity="recent_tracks",
    payload=recent_tracks,
    downloaded_at=downloaded_at
    )
    
    return downloaded_at