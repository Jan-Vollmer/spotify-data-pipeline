import duckdb
import pandas as pd
from pathlib import Path

class DuckDBHelper:
    def __init__(self, scope: str):
        self.con = duckdb.connect()
        self.scope = scope
        gold_path = f"data/gold/{self.scope}/*.parquet"
        self.con.execute(f"CREATE VIEW top_{self.scope} AS SELECT * FROM read_parquet('{gold_path}')")

    def query(self, sql: str) -> pd.DataFrame:
        return self.con.execute(sql).df()

    def top_n_by_term(self, n: int = 10) -> pd.DataFrame:
        sql = f"""
            SELECT id, name, position, term
            FROM top_{self.scope}
            WHERE position <= {n} AND
            snapshot_date = (SELECT MAX(snapshot_date) FROM top_top_artists)
            ORDER BY term, position
        """
        return self.query(sql)
    
    def top_scope_over_time(self, id: str) -> pd.DataFrame:
        sql = f"""
            SELECT id, name, position, term
            FROM top_{self.scope}
            WHERE id = '{id}' AND
            snapshot_date = (SELECT MAX(snapshot_date) FROM top_top_artists)
            ORDER BY snapshot_date
        """
        return self.query(sql)