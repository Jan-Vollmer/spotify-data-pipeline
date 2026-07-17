EXPECTED_FIELDS = {
    "top_tracks": ["id", "name", "artists", "album.id", "album.name", "album.artists", "duration_ms"],
    "top_artists": ["id", "name", "genres", "popularity"],
    "recent_tracks": ["played_at", "track.id", "track.name", "track.artists",
                       "track.album.id", "track.album.name", "track.album.artists", "track.duration_ms"],
}

def job_to_scope(job_name: str) -> str:
    for suffix in ("_short", "_medium", "_long"):
        if job_name.endswith(suffix):
            return job_name[: -len(suffix)]
    return job_name

def _get_nested(item: dict, dotted_path: str):
    node = item
    for part in dotted_path.split("."):
        if not isinstance(node, dict) or part not in node:
            return None, False
        node = node[part]
    return node, True

def validate_item_schema(item: dict, job_name: str) -> list[str]:
    scope = job_to_scope(job_name)
    expected = EXPECTED_FIELDS.get(scope, [])
    missing = []
    for path in expected:
        _, found = _get_nested(item, path)
        if not found:
            missing.append(path)
    return missing