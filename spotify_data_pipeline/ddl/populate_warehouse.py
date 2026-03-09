from spotify_data_pipeline.helpers.duckdb_helper import DuckDBHelper

db = DuckDBHelper("data/warehouse.duckdb")

# dim_artist
db.con.execute("""
    INSERT OR IGNORE INTO dim_artist (artist_id, artist_name)
    SELECT DISTINCT
        id AS artist_id,
        name AS artist_name
    FROM read_parquet('data/gold/top_artists/*.parquet')
""")

# dim_genre
db.con.execute("""
    INSERT OR IGNORE INTO dim_genre (genre_name)
    SELECT DISTINCT genre
    FROM read_parquet('data/gold/top_artists/*.parquet'),
         UNNEST(genres) AS genre
""")

# dim_artist_genre
db.con.execute("""
    INSERT OR IGNORE INTO artist_genre (artist_id, genre_name)
    SELECT id AS artist_id, genre
    FROM read_parquet('data/gold/top_artists/*.parquet'),
         UNNEST(genres) AS genre
""")

# dim_track
db.con.execute("""
    INSERT OR IGNORE INTO dim_track (
        track_id,
        track_name,
        artist_id,
        album_id,
        album_name,
        album_release_date,
        duration_ms,
        explicit,
        popularity
    )
    SELECT DISTINCT
        id AS track_id,
        name AS track_name,
        artist_ids[1] AS artist_id,
        album_id,
        album_name,
        album_release_date::DATE,
        duration_ms,
        explicit,
        popularity
    FROM read_parquet('data/gold/top_tracks/*.parquet')
""")

# fact_recent_tracks
db.con.execute("""
    INSERT OR IGNORE INTO fact_recent_tracks (track_id, artist_id, played_at, context_type, context)
    SELECT
        id AS track_id,
        artist_ids[1] AS artist_id,
        TO_TIMESTAMP(played_at/1000) AS played_at,
        context_type,
        context
        FROM read_parquet('data/gold/recent_tracks/*.parquet')                 
    """)

# fact_artist_rankings
db.con.execute("""
    INSERT INTO fact_artist_rankings (artist_id, term_id, position, snapshot_date)
    SELECT
        id AS artist_id,
        term AS term_id,
        position,
        snapshot_date::DATE
    FROM read_parquet('data/gold/top_artists/*.parquet')
""")

# fact_track_rankings
db.con.execute("""
    INSERT INTO fact_track_rankings (track_id, term_id, position, snapshot_date)
    SELECT
        id AS track_id,
        term AS term_id,
        position,
        snapshot_date::DATE
    FROM read_parquet('data/gold/top_tracks/*.parquet')
""")

# dim_term
db.con.execute("""
    INSERT INTO dim_term (term_id, term_name)
    SELECT DISTINCT
        term AS term_id,
        CASE term
            WHEN 'l' THEN 'long'
            WHEN 'm' THEN 'medium'
            WHEN 's' THEN 'short'
        END AS term_name
    FROM read_parquet('data/gold/top_artists/*.parquet')
""")