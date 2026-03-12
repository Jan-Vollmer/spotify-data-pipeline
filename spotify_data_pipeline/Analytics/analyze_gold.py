from spotify_data_pipeline.helpers.duckdb_helper import DuckDBHelper
from spotify_data_pipeline.helpers.query_helper import *

def main():
    db = DuckDBHelper("data/warehouse.duckdb")
    
    print("top_n_artist_by_term:")
    print(top_n_artist_by_term(db, 10))

    print("top_scope_over_time:")
    print(top_scope_over_time(db, "Lacrimosa"))

    print("biggest_rank_improvement")
    print(biggest_rank_improvement(db))

    print("most_stable_artists")
    print(most_stable_artists(db))

    print("new_artists")
    print(new_artists(db))

    print("most_stable_track_per_term")
    print(most_stable_track_per_term(db))

    print("top_track_over_time")
    print(top_track_over_time(db, "Der Morgen danach"))

    db.con.close()

if __name__ == "__main__":
    main()