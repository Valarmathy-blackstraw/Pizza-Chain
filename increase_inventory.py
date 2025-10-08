import time
import pandas as pd
from db_setup_demo import client, CH_DB

while True:
    # Fetch all rows
    si_query = client.query(f"""
           SELECT * FROM {CH_DB}.store_inventory FINAL """)
    inventory_df = pd.DataFrame(si_query.result_rows, columns=si_query.column_names)
    inventory_updates = []
    # Prepare updated rows
    for _, si_row in inventory_df.iterrows():
        new_stock_in = si_row['stock_in'] + 1000
        inventory_updates.append((si_row['store_id'], si_row['ingredient_id'], new_stock_in, si_row['stock_out']))
    # Insert updated rows (ReplacingMergeTree will handle deduplication based on updated_at)
    for store_id,ingredient_id, new_stock_in, stock_out in inventory_updates:
        client.command(f"""
            INSERT INTO {CH_DB}.store_inventory (store_id, ingredient_id, stock_in, stock_out, updated_at)
            VALUES ({store_id}, {ingredient_id}, {new_stock_in}, {stock_out},now())
        """)
    print("✅ stock_in increased by 1000 for all rows.")

    time.sleep(300)
