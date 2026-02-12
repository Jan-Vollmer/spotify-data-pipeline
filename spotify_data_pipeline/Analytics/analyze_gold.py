from helpers.duckdb_helper import DuckDBHelper

def main():
    db = DuckDBHelper("top_artists")
    
    df_top10 = db.top_n_by_term(10)
    print("Top 10 Artists je Term:")
    print(df_top10)
    
    artist_id = "57ekbx9PSS4ORs5wTZMSYp"
    df_history = db.top_scope_over_time(artist_id)
    print(f"Verlauf von Artist {artist_id}:")
    print(df_history)

if __name__ == "__main__":
    main()