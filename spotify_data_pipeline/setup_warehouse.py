from spotify_data_pipeline.helpers.duckdb_helper import DuckDBHelper

db = DuckDBHelper("data/warehouse.duckdb")
db.run_sql_file("spotify_data_pipeline/ddl/warehouse.sql")