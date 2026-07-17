from spotify_data_pipeline.helpers.schema_validation import validate_item_schema

def test_validate_item_schema_top_tracks_missing_nested_field():
    item = {"id": "1", "name": "Song", "artists": [], "album": {"id": "a1"}}  # album.name, album.artists, duration_ms fehlen
    missing = validate_item_schema(item, "top_tracks_short")
    assert "album.name" in missing
    assert "duration_ms" in missing
    assert "album.id" not in missing

def test_validate_item_schema_recent_tracks_nested():
    item = {"played_at": "2026-07-17", "track": {"id": "1", "name": "Song"}}
    missing = validate_item_schema(item, "recent_tracks")
    assert "track.artists" in missing
    assert "track.album.id" in missing
    assert "track.id" not in missing

def test_validate_item_schema_top_artists_complete():
    item = {"id": "1", "name": "Artist", "genres": ["pop"], "popularity": 80}
    missing = validate_item_schema(item, "top_artists_short")
    assert missing == []