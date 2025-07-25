import duckdb
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Union
import os 

class DuckDBClient:
    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self.con = duckdb.connect(str(db_path))


    def load_csv_as_table(self, csv_path: str, table_name: str):
        self.con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS 
            SELECT * FROM read_csv_auto('{csv_path}', HEADER=TRUE);
        """)
    def load_parquet_as_table(self, parquet_path: Union[str, Path], table_name: str):
        self.con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_path}')")

    def register_df(self, table_name: str, df: pd.DataFrame):
        self.con.register(table_name, df)


    def run_query(self, sql: str) -> pd.DataFrame:
        return self.con.execute(sql).df()

    def run_inplace(self, sql: str):
        self.con.execute(sql)

    def save_to_parquet(
        self,
        df: pd.DataFrame,
        path: Union[str, Path],
        overwrite: bool = True
    ):
        path = Path(path)
        abs_path = path.resolve()
        print(f"📂 Checking: {abs_path} | Exists: {path.exists()}")

        path.parent.mkdir(parents=True, exist_ok=True)

        if path.exists():
            try:
                print(f"🗑️ Deleting existing file: {abs_path}")
                os.remove(abs_path)
            except Exception as e:
                print(f"❌ Failed to delete {abs_path}: {e}")
                raise

        try:
            print(f"💾 Writing {len(df):,} rows to {abs_path}")
            df.to_parquet(abs_path, index=False, compression="snappy")
        except Exception as e:
            print(f"❌ Failed to write parquet to {abs_path}: {e}")
            raise



    def save_to_parquet(
        self,
        df: pd.DataFrame,
        path: Union[str, Path],
        overwrite: bool = False,
        dedup_cols: Optional[list] = None,
    ):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        if overwrite or not path.exists():
            df.to_parquet(path, index=False)
            return

        # Load existing and append
        existing_df = pd.read_parquet(path)
        combined_df = pd.concat([existing_df, df], ignore_index=True)

        if dedup_cols:
            combined_df.drop_duplicates(subset=dedup_cols, keep="last", inplace=True)

        combined_df.to_parquet(path, index=False)
    def close(self):
        self.con.close()