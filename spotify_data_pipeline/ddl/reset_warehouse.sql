-- manuel reset of all tables after schema changes
-- 1. FACTS
DROP TABLE IF EXISTS fact_artist_rankings CASCADE;
DROP TABLE IF EXISTS fact_track_rankings CASCADE;
DROP TABLE IF EXISTS fact_recent_tracks CASCADE;

-- 2. BRIDGES
DROP TABLE IF EXISTS bridge_artist_genre CASCADE;
DROP TABLE IF EXISTS bridge_track_artist CASCADE;

-- 3. DIMENSIONS
DROP TABLE IF EXISTS dim_track CASCADE;
DROP TABLE IF EXISTS dim_album CASCADE;
DROP TABLE IF EXISTS dim_genre CASCADE;
DROP TABLE IF EXISTS dim_term CASCADE;
DROP TABLE IF EXISTS dim_artist CASCADE;