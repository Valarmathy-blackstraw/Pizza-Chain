
from datetime import datetime
import os
import pandas as pd
import clickhouse_connect as conn
import config

# ------------------------
# ClickHouse Cloud Setup
# ------------------------
CH_DB = "PIZZA"
client = conn.get_client(
    host=config.CLICKHOUSE_CONF['host'],
    port=config.CLICKHOUSE_CONF['port'],
    user=config.CLICKHOUSE_CONF['user'],
    password=config.CLICKHOUSE_CONF['password']
)


def load_csv(table, path, extra_id=None):
    print(f"📂 Reading CSV: {path}")
    df = pd.read_csv(path)
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].fillna("")
    if extra_id:
        df.insert(0, extra_id, range(1, len(df) + 1))
    for col in df.columns:
        if "updated_at" in col.lower():
            # df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
            # df[col] = pd.to_datetime(df[col])
            df[col] = datetime.now()
    client.insert_df(f"{CH_DB}.{table}", df)
    print(f"✅ Loaded {len(df)} rows into {table}")

csv_path = r"D:/Pizza/InputData"

if __name__ == "__main__":
    with open("Queries.sql", "r") as f:
      sql_content = f.read()
    queries = [q.strip() for q in sql_content.split(";") if q.strip()]
    for q in queries:
        try:
            client.command(q)
            print(f"✅ Executed: {q.splitlines()[0]}")
        except Exception as e:
            print(f"❌ Failed: {q}\n {e}")
    print("Loading CSV data...")
    load_csv("users", f"{csv_path}/users.csv", extra_id="user_id")
    load_csv("products", f"{csv_path}/products.csv", extra_id="product_id")
    load_csv("stores", f"{csv_path}/stores.csv")
    load_csv("ingredients", f"{csv_path}/ingredients.csv")
    load_csv("product_ingredients", f"{csv_path}/product_ingredients.csv")
    load_csv("store_inventory", f"{csv_path}/store_inventory.csv")

    print("✅ Setup complete")
