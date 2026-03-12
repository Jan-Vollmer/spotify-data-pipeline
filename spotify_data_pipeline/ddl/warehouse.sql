CREATE TABLE IF NOT EXISTS dim_artist (
    artist_id VARCHAR PRIMARY KEY,
    artist_name VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_track (
    track_id VARCHAR PRIMARY KEY,
    track_name VARCHAR,
    album_id VARCHAR,
    duration_ms INTEGER,
    explicit BOOLEAN,
    track_number INTEGER,
    disc_number INTEGER
);

CREATE TABLE IF NOT EXISTS dim_term (
    term_id VARCHAR PRIMARY KEY,
    term_name VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_genre (
    genre_name VARCHAR PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS dim_album (
    album_id VARCHAR PRIMARY KEY,
    album_name VARCHAR,
    album_type VARCHAR,
    album_total_tracks INTEGER
);

CREATE TABLE IF NOT EXISTS bridge_artist_genre (
    artist_id VARCHAR,
    genre_name VARCHAR,
    FOREIGN KEY (artist_id) REFERENCES dim_artist(artist_id),
    FOREIGN KEY (genre_name) REFERENCES dim_genre(genre_name),
    PRIMARY KEY (artist_id, genre_name)
);

CREATE TABLE IF NOT EXISTS bridge_track_artist(
    track_id VARCHAR,
    artist_id VARCHAR,
    FOREIGN KEY (track_id) REFERENCES dim_track(track_id),
    FOREIGN KEY (artist_id) REFERENCES dim_artist(artist_id),
    PRIMARY KEY (artist_id, track_id)
);

CREATE TABLE IF NOT EXISTS fact_artist_rankings (
    artist_id VARCHAR,
    term_id VARCHAR,
    position INTEGER,
    snapshot_date TIMESTAMP,
    PRIMARY KEY (artist_id, term_id, snapshot_date, position),
    FOREIGN KEY (artist_id) REFERENCES dim_artist(artist_id),
    FOREIGN KEY (term_id) REFERENCES dim_term(term_id)
);

CREATE TABLE IF NOT EXISTS fact_track_rankings (
    track_id VARCHAR,
    term_id VARCHAR,
    position INTEGER,
    snapshot_date TIMESTAMP,
    PRIMARY KEY (track_id, term_id, snapshot_date, position),
    FOREIGN KEY (track_id) REFERENCES dim_track(track_id),
    FOREIGN KEY (term_id) REFERENCES dim_term(term_id)
);

CREATE TABLE IF NOT EXISTS fact_recent_tracks (
    track_id VARCHAR,
    played_at TIMESTAMP,
    context_type VARCHAR,
    context VARCHAR,
    PRIMARY KEY (track_id, played_at),
    FOREIGN KEY (track_id) REFERENCES dim_track(track_id)
);