import os
from dotenv import load_dotenv

load_dotenv()

from spotify_data_pipeline.helpers.duckdb_helper import DuckDBHelper

db = DuckDBHelper("data/warehouse.duckdb")

db.con.execute("INSTALL azure")
db.con.execute("LOAD azure")

db.con.execute(f"""
CREATE OR REPLACE SECRET azure_conn (
    TYPE azure,
    CONNECTION_STRING '{os.environ["AZURE_CONNECTION_STRING"]}'
)
""")

# -----------------------------------------------------------------------------
# Dimensions
# -----------------------------------------------------------------------------

# dim_artist — from top_artists
db.con.execute("""
INSERT OR IGNORE INTO dim_artist (artist_id, artist_name)
SELECT DISTINCT id AS artist_id, name AS artist_name
FROM read_parquet([
    'az://gold/top_artists/short/*.parquet',
    'az://gold/top_artists/medium/*.parquet',
    'az://gold/top_artists/long/*.parquet'
])
""")

# dim_artist — from recent_tracks
db.con.execute("""
INSERT OR IGNORE INTO dim_artist (artist_id, artist_name)
SELECT DISTINCT artist_id, artist_name
FROM read_parquet('az://gold/recent_tracks/**/*.parquet')
""")

# dim_artist — from top_tracks
db.con.execute("""
INSERT OR IGNORE INTO dim_artist (artist_id, artist_name)
SELECT DISTINCT artist_id, artist_name
FROM read_parquet([
    'az://gold/top_tracks/short/*.parquet',
    'az://gold/top_tracks/medium/*.parquet',
    'az://gold/top_tracks/long/*.parquet'
])
""")

# dim_track
db.con.execute("""
INSERT OR IGNORE INTO dim_track (
    track_id,
    track_name,
    album_id,
    duration_ms,
    explicit,
    track_number,
    disc_number
)
SELECT DISTINCT
    track_id,
    track_name,
    album_id,
    duration_ms,
    explicit,
    track_number,
    disc_number
FROM read_parquet([
    'az://gold/top_tracks/short/*.parquet',
    'az://gold/top_tracks/medium/*.parquet',
    'az://gold/top_tracks/long/*.parquet'
])

UNION

SELECT DISTINCT
    track_id,
    track_name,
    album_id,
    duration_ms,
    explicit,
    track_number,
    album_disc_number AS disc_number
FROM read_parquet('az://gold/recent_tracks/**/*.parquet')
""")

# dim_album
db.con.execute("""
INSERT OR IGNORE INTO dim_album (
    album_id,
    album_name,
    album_type,
    album_total_tracks
)
SELECT DISTINCT
    album_id,
    album_name,
    album_type,
    album_total_tracks
FROM read_parquet([
    'az://gold/top_tracks/short/*.parquet',
    'az://gold/top_tracks/medium/*.parquet',
    'az://gold/top_tracks/long/*.parquet'
])

UNION

SELECT DISTINCT
    album_id,
    album_name,
    album_type,
    album_total_tracks
FROM read_parquet('az://gold/recent_tracks/**/*.parquet')
""")

# dim_genre
db.con.execute("""
INSERT OR IGNORE INTO dim_genre (genre_name)
SELECT DISTINCT genre_name
FROM read_parquet([
    'az://gold/top_artists/short/*.parquet',
    'az://gold/top_artists/medium/*.parquet',
    'az://gold/top_artists/long/*.parquet'
]) t,
UNNEST(t.genres) AS genre_unnest(genre_name)
""")

# dim_term
db.con.execute("""
INSERT OR IGNORE INTO dim_term (term_id, term_name)
VALUES
    ('s', 'short'),
    ('m', 'medium'),
    ('l', 'long')
""")

# -----------------------------------------------------------------------------
# Bridge Tables
# -----------------------------------------------------------------------------

# bridge_artist_genre
db.con.execute("""
INSERT OR IGNORE INTO bridge_artist_genre (artist_id, genre_name)
SELECT
    id AS artist_id,
    genre_name
FROM read_parquet([
    'az://gold/top_artists/short/*.parquet',
    'az://gold/top_artists/medium/*.parquet',
    'az://gold/top_artists/long/*.parquet'
]) t,
UNNEST(t.genres) AS genre_unnest(genre_name)
""")

# bridge_track_artist
db.con.execute("""
INSERT OR IGNORE INTO bridge_track_artist (track_id, artist_id)
SELECT DISTINCT
    track_id,
    artist_id
FROM read_parquet([
    'az://gold/top_tracks/short/*.parquet',
    'az://gold/top_tracks/medium/*.parquet',
    'az://gold/top_tracks/long/*.parquet'
])
""")

# -----------------------------------------------------------------------------
# Facts
# -----------------------------------------------------------------------------

# fact_artist_rankings
db.con.execute("""
INSERT OR IGNORE INTO fact_artist_rankings (
    artist_id,
    term_id,
    position,
    snapshot_date
)

SELECT DISTINCT
    id AS artist_id,
    's' AS term_id,
    position,
    snapshot_date
FROM read_parquet('az://gold/top_artists/short/*.parquet')

UNION ALL

SELECT DISTINCT
    id AS artist_id,
    'm' AS term_id,
    position,
    snapshot_date
FROM read_parquet('az://gold/top_artists/medium/*.parquet')

UNION ALL

SELECT DISTINCT
    id AS artist_id,
    'l' AS term_id,
    position,
    snapshot_date
FROM read_parquet('az://gold/top_artists/long/*.parquet')
""")

# fact_track_rankings
db.con.execute("""
INSERT OR IGNORE INTO fact_track_rankings (
    track_id,
    term_id,
    position,
    snapshot_date
)

SELECT DISTINCT
    track_id,
    's' AS term_id,
    position,
    snapshot_date
FROM read_parquet('az://gold/top_tracks/short/*.parquet')

UNION ALL

SELECT DISTINCT
    track_id,
    'm' AS term_id,
    position,
    snapshot_date
FROM read_parquet('az://gold/top_tracks/medium/*.parquet')

UNION ALL

SELECT DISTINCT
    track_id,
    'l' AS term_id,
    position,
    snapshot_date
FROM read_parquet('az://gold/top_tracks/long/*.parquet')
""")

# fact_recent_tracks
db.con.execute("""
INSERT OR IGNORE INTO fact_recent_tracks (
    track_id,
    played_at,
    context_type,
    context
)
SELECT DISTINCT
    track_id,
    played_at,
    context_type,
    context
FROM read_parquet('az://gold/recent_tracks/**/*.parquet')
""")

db.con.close()