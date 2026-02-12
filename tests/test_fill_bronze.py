from spotify_data_pipeline.Bronze.fill_bronze import fill_bronze

def test_fill_bronze_smoke(monkeypatch):
    """
    Smoke test for the orchestration layer.

    fill_bronze contains no business logic and only coordinates
    already unit-tested helpers. This test ensures the orchestration
    runs end-to-end without raising and returns a timestamp.
    """
    monkeypatch.setattr("spotify_data_pipeline.Bronze.fill_bronze.get_or_refresh_token", lambda _: "token")
    monkeypatch.setattr("spotify_data_pipeline.Bronze.fill_bronze.fetch_and_write", lambda *a, **k: None)
    monkeypatch.setattr("spotify_data_pipeline.Bronze.fill_bronze.get_recent_tracks", lambda *a, **k: [])
    monkeypatch.setattr("spotify_data_pipeline.Bronze.fill_bronze.write_bronze_batch", lambda *a, **k: None)

    ts = fill_bronze()

    assert isinstance(ts, str)