import json
from pathlib import Path
from datetime import datetime
from auth import get_or_refresh_token
from get_top_tracks import get_top_tracks
from get_top_artists import get_top_artists
from get_last_played import get_last_played
from bronze_helper import write_bronze_batch

def fill_bronze(limit_top: int = None, limit_recent: int = None, time_range: str = None):
    
    Path("data/bronze").mkdir(parents=True, exist_ok=True)
    
    access_top = get_or_refresh_token("user-top-read")
    access_recent = get_or_refresh_token("user-read-recently-played")
    
    top_tracks = get_top_tracks(access_top, limit=limit_top, time_range=time_range)
    top_artists = get_top_artists(access_top, limit=limit_top, time_range=time_range)
    recent_tracks = get_last_played(access_recent, limit=limit_recent)
    
    print("\nTop Tracks:")
    for i, t in enumerate(top_tracks, start=1):
        print(f"{i}. {t['name']} – {t['artists'][0]['name']}")
    
    print("\nTop Artists:")
    for i, a in enumerate(top_artists, start=1):
        genres = ", ".join(a.get("genres", []))
        print(f"{i}. {a['name']} ({genres})")
    
    print("\nZuletzt gespielt:")
    for i, r in enumerate(recent_tracks, start=1):
        track = r["track"]
        print(f"{i}. {track['name']} – {track['artists'][0]['name']}")
    
    downloaded_at = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")

    write_bronze_batch(
    entity="top_tracks",
    payload=top_tracks,
    downloaded_at=downloaded_at,
    subdir=f"time_range={time_range or 'default'}"
    )
    
    write_bronze_batch(
    entity="top_artists",
    payload=top_artists,
    downloaded_at=downloaded_at,
    subdir=f"time_range={time_range or 'default'}"
    )
    
    write_bronze_batch(
    entity="recent_tracks",
    payload=recent_tracks,
    downloaded_at=downloaded_at
    )
    
    return top_tracks, top_artists, recent_tracks