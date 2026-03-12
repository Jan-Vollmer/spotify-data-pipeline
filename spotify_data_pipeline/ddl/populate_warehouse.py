from spotify_data_pipeline.helpers.duckdb_helper import DuckDBHelper

db = DuckDBHelper("data/warehouse.duckdb")

# dim_artist
db.con.execute("""
    INSERT OR IGNORE INTO dim_artist (artist_id, artist_name)
    SELECT DISTINCT id AS artist_id, name AS artist_name
    FROM read_parquet('data/gold/top_artists/*.parquet')
""")

db.con.execute("""
   INSERT OR IGNORE INTO dim_artist (artist_id, artist_name)
    SELECT DISTINCT artist_id, artist_name
    FROM read_parquet('data/gold/recent_tracks/*.parquet')
 """)

db.con.execute("""
   INSERT OR IGNORE INTO dim_artist (artist_id, artist_name)
    SELECT DISTINCT artist_id, artist_name
    FROM read_parquet('data/gold/top_tracks/*.parquet')
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
    FROM read_parquet('data/gold/top_tracks/*.parquet')
    UNION
    SELECT DISTINCT
        track_id,
        track_name,
        album_id,
        duration_ms,
        explicit,
        track_number,
        album_disc_number as disc_number        
    FROM read_parquet('data/gold/recent_tracks/*.parquet')                      
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
        FROM read_parquet('data/gold/top_tracks/*.parquet')
        UNION
        SELECT DISTINCT 
            album_id,
            album_name,
            album_type,               
            album_total_tracks
        FROM read_parquet('data/gold/recent_tracks/*.parquet')                         
""")

# dim_genre
db.con.execute("""
    INSERT OR IGNORE INTO dim_genre (genre_name)
    SELECT DISTINCT genre
    FROM read_parquet('data/gold/top_artists/*.parquet'),
        UNNEST(genres) AS genre
""")

# dim_term
db.con.execute("""
    INSERT OR IGNORE INTO dim_term (term_id, term_name)
    SELECT DISTINCT
        term AS term_id,
        CASE term
            WHEN 'l' THEN 'long'
            WHEN 'm' THEN 'medium'
            WHEN 's' THEN 'short'
        END AS term_name
    FROM read_parquet('data/gold/top_artists/*.parquet')
""")

# bridge_artist_genre
db.con.execute("""
    INSERT OR IGNORE INTO bridge_artist_genre (artist_id, genre_name)
    SELECT id AS artist_id, genre as genre_name
    FROM read_parquet('data/gold/top_artists/*.parquet'),
        UNNEST(genres) AS genre
""")

# bridge_track_artist
db.con.execute("""
    INSERT OR IGNORE INTO bridge_track_artist(track_id, artist_id)
    SELECT DISTINCT track_id, artist_id
    FROM read_parquet('data/gold/top_tracks/*.parquet'),
""")

# fact_artist_rankings
db.con.execute("""
    INSERT INTO fact_artist_rankings (artist_id, term_id, position, snapshot_date)
    SELECT DISTINCT
        id AS artist_id,
        term AS term_id,
        position,
        snapshot_date
    FROM read_parquet('data/gold/top_artists/*.parquet')
""")

# fact_track_rankings
db.con.execute("""
    INSERT INTO fact_track_rankings (track_id, term_id, position, snapshot_date)
    SELECT DISTINCT
        track_id,
        term AS term_id,
        position,
        snapshot_date
    FROM read_parquet('data/gold/top_tracks/*.parquet')
""")

# fact_recent_tracks
db.con.execute("""
    INSERT OR IGNORE INTO fact_recent_tracks (track_id, played_at, context_type, context)
    SELECT DISTINCT
        track_id,
        played_at,
        context_type,
        context
        FROM read_parquet('data/gold/recent_tracks/*.parquet')                 
    """)

db.con.close()