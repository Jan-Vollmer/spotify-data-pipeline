from helpers.duckdb_helper import DuckDBHelper

def main():
    db = DuckDBHelper("top_artists")
    
    df_top10 = db.top_n_by_term(10)
    print("Top 10 Artists je Term:")
    print(df_top10)
    
    artist_name = "Lacrimosa"
    df_history = db.top_scope_over_time(artist_name)
    print(f"Verlauf von Artist {artist_name}:")
    print(df_history)

if __name__ == "__main__":
    main()