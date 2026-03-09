import duckdb
from pathlib import Path

class DuckDBHelper:
    def __init__(self, db_path: str = "data/warehouse.duckdb"):
        Path("data").mkdir(exist_ok=True)
        self.con = duckdb.connect(db_path)


    def run_sql_file(self, path: str):
        with open(path) as f:
            self.con.execute(f.read())

    def create_parquet_view(self, view_name: str, parquet_glob: str):
        self.con.execute(f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT * FROM read_parquet('{parquet_glob}')
        """)

    def query(self, sql: str):
        return self.con.execute(sql).df()