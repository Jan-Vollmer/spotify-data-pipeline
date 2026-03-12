from spotify_data_pipeline.helpers.duckdb_helper import DuckDBHelper

def top_n_artist_by_term(db: DuckDBHelper, n: int):
    query = f"""
        SELECT
            term_id,
            position,
            a.artist_name
        FROM fact_artist_rankings f
        JOIN dim_artist a
        ON f.artist_id = a.artist_id
        WHERE 1=1
        AND position <= {n}
        AND NOT EXISTS (
              SELECT 1
              FROM fact_artist_rankings f2
              WHERE f2.term_id = f.term_id
                AND f2.artist_id = f.artist_id
                AND f2.snapshot_date > f.snapshot_date
          )
        ORDER BY term_id, position
    """
    return db.con.execute(query).df()

def top_scope_over_time(db: DuckDBHelper, artist_name: str):
    query = """
        SELECT
            a.artist_name,
            f.term_id,
            f.snapshot_date,
            f.position
        FROM fact_artist_rankings f
        JOIN dim_artist a
        ON f.artist_id = a.artist_id
        WHERE 1=1
        AND a.artist_name = ?
        AND NOT EXISTS (
              SELECT 1
              FROM fact_artist_rankings f2
              WHERE f2.term_id = f.term_id
                AND f2.artist_id = f.artist_id
                AND f2.snapshot_date > f.snapshot_date
          )
        ORDER BY f.term_id, f.snapshot_date
    """
    return db.con.execute(query, [artist_name]).df()

def biggest_rank_improvement(db: DuckDBHelper):
    query = """
        SELECT *
        FROM (
            SELECT
                a.artist_name,
                term_id,
                snapshot_date,
                position,
                position - LAG(position) OVER(
                    PARTITION BY artist_id, term_id
                    ORDER BY snapshot_date
                ) AS delta
            FROM fact_artist_rankings f
            JOIN dim_artist a
            USING(artist_id)
        )
        WHERE delta < 0
        ORDER BY delta
        LIMIT 20
    """
    return db.con.execute(query).df()

def most_stable_artists(db: DuckDBHelper):
    query = """
        SELECT
            a.artist_name,
            term_id,
            STDDEV(position) AS rank_volatility
        FROM fact_artist_rankings f
        JOIN dim_artist a USING(artist_id)
        GROUP BY a.artist_name, term_id
        HAVING COUNT(*) > 3
        ORDER BY rank_volatility
        LIMIT 20
    """
    return db.con.execute(query).df()

def new_artists(db: DuckDBHelper):
    query = """
        SELECT
            a.artist_name,
            MIN(snapshot_date) AS first_seen,
            f.term_id
        FROM fact_artist_rankings f
        JOIN dim_artist a USING(artist_id)
        GROUP BY a.artist_name, f.term_id
        ORDER BY first_seen DESC
        LIMIT 20
    """
    return db.con.execute(query).df()

def most_stable_track_per_term(db: DuckDBHelper):
    query = """
        WITH track_vol AS (
            SELECT
                t.track_name,
                f.term_id,
                STDDEV(f.position) AS rank_volatility
            FROM fact_track_rankings f
            JOIN dim_track t
            ON f.track_id = t.track_id
            GROUP BY t.track_name, f.term_id
        )
        SELECT track_name, term_id, rank_volatility
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY term_id ORDER BY rank_volatility ASC) AS rn
            FROM track_vol
        ) sub
        WHERE rn = 1
        ORDER BY term_id;
    """
    return db.con.execute(query).df()

def top_track_over_time(db: DuckDBHelper, track_name: str):
    query = """
        SELECT t.track_name, f.term_id, f.snapshot_date, f.position
        FROM fact_track_rankings f
        JOIN dim_track t
        ON f.track_id = t.track_id
        WHERE t.track_name = ?
        ORDER BY f.term_id, f.snapshot_date;
    """
    return db.con.execute(query, [track_name]).df()