import time
import pandas as pd
from db_setup_demo import client, CH_DB

while True:
    # Step 1: Fetch low_stock alerts
    si_query = client.query(f"""
        SELECT store_id, ingredient_id, current_stock, sold_out
        FROM {CH_DB}.alert_table FINAL
        WHERE alert_type = 'low_stock'
    """)

    # Step 2: Convert to DataFrame
    inventory_df = pd.DataFrame(si_query.result_rows, columns=si_query.column_names)

    # Step 3: Prepare updated rows
    inventory_updates = []
    stock_log_entries = []

    for _, row in inventory_df.iterrows():
        new_stock_in = row['current_stock'] + 1000
        inventory_updates.append((row['store_id'], row['ingredient_id'], new_stock_in, row['sold_out']))
        stock_log_entries.append((row['store_id'], row['ingredient_id'], 1000))

    # Step 4: Insert updated rows into store_inventory
    for store_id, ingredient_id, new_stock_in, stock_out in inventory_updates:
        client.command(f"""
            INSERT INTO {CH_DB}.store_inventory (store_id, ingredient_id, stock_in, stock_out, updated_at)
            VALUES ({store_id}, {ingredient_id}, {new_stock_in}, {stock_out}, now())
        """)
        print(f"stock_in increased by 1000 for store- {store_id} ingredient- {ingredient_id}")

        # Step 5: Log into total_stocks_added
    for store_id, ingredient_id, added_qty in stock_log_entries:
        client.command(f"""
               INSERT INTO {CH_DB}.total_stocks_added (store_id, ingredient_id, total_stocks_added, updated_at)
               VALUES ({store_id}, {ingredient_id}, {added_qty}, now())
           """)
        print(f"stock_in logged for store- {store_id} ingredient- {ingredient_id}")

    # print("✅ stock_in increased by 1000 for  low_stock ingredients.")

    # time.sleep(300)
