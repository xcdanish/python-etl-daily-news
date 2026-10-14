import duckdb
import os

os.makedirs("data", exist_ok=True)
con = duckdb.connect("data/dagster_tutorial.duckdb")
con.execute("CREATE TABLE IF NOT EXISTS dummy_init (id INT);")
con.close()
print("DuckDB warehouse initialized.")
