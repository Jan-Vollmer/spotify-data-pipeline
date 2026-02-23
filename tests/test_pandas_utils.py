import json
import pandas as pd
from pandas import DataFrame
from pathlib import Path
from spotify_data_pipeline.helpers.pandas_utils import transform_silver_artist, append_to_parquet, load_jsons_to_df, dedupe_df, transform_silver_track, transform_silver_recent_track

def test_load_jsons_to_df(tmp_path):
    file1 = tmp_path / "empty.json"
    file1.write_text("[]")

    file2 = tmp_path / "data.json"
    file2.write_text('[{"artist":"a"},{"artist":"b"}]')

    df_empty = load_jsons_to_df([file1])
    assert df_empty.empty

    df = load_jsons_to_df([file2])
    assert len(df) == 2
    assert "artist" in df.columns

def test_dedupe_df():
    df = DataFrame({"played_at": [1, 2, 1], "track": ["a","b","a"]})
    df2 = dedupe_df(df.copy(), subset="played_at")
    assert len(df2) == 2  

def test_append_to_parquet(tmp_path):
    df1 = pd.DataFrame({"played_at":[1,2], "track":["a","b"]})
    parquet_file = tmp_path / "test.parquet"

    combined1 = append_to_parquet(df1, parquet_file)
    assert parquet_file.exists()
    assert len(combined1) == 2

    df2 = pd.DataFrame({"played_at":[2,3], "track":["b","c"]})
    combined2 = append_to_parquet(df2, parquet_file)
    assert len(combined2) == 3    

def test_transform_silver_artist():
    df = DataFrame({
        "artist":["a"],
        "available_markets":["x"],
        "images":["y"],
        "type":["z"]
    })
    df2 = transform_silver_artist(df)
    assert "artist" in df2.columns
    assert "available_markets" not in df2.columns
    assert "images" not in df2.columns
    assert "type" not in df2.columns    

def test_transform_silver_track():
    df = DataFrame({
        "track.artists": [[{"id":"a","name":"n","type":"t"}]],
        "track.album.artists": [[{"id":"b","name":"m"}]],
        "track.id":["id1"],
        "track.name":["song1"],
        "album.images":["img"],
        "track.preview_url":["url"]
    })
    df2 = transform_silver_track(df)
    assert "artists" not in df2.columns
    assert "album_artist_ids" in df2.columns
    assert "artist_ids" in df2.columns   

def test_transform_silver_recent_track():
    df = DataFrame({
        "track.artists":[[{"id":"a","name":"n","type":"t"}]]*3,
        "track.album.artists":[[{"id":"b","name":"m"}]]*3,
        "artist_ids":[None]*3,
        "played_at":[1,2,1]
    })
    df2 = transform_silver_recent_track(df)
    assert df2.shape[0] == 2    