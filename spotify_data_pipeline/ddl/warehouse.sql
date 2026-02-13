CREATE TABLE IF NOT EXISTS dim_artist (
    artist_id VARCHAR PRIMARY KEY,
    artist_name VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_track (
    track_id VARCHAR PRIMARY KEY,
    track_name VARCHAR,
    artist_id VARCHAR
);

CREATE TABLE IF NOT EXISTS fact_artist_rankings (
    artist_id VARCHAR,
    term VARCHAR,
    position INTEGER,
    snapshot_date DATE
);

CREATE TABLE IF NOT EXISTS fact_track_rankings (
    track_id VARCHAR,
    term VARCHAR,
    position INTEGER,
    snapshot_date DATE
);