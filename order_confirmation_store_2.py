import time
import pandas as pd
from db_setup_demo import client, CH_DB


def process_order(client, ch_db, order_row):
    order_id = order_row['order_id']
    orders_query = client.query(f"""
        SELECT * FROM {CH_DB}.orders WHERE order_id = {order_id} limit 1
    """)
    orders_df = pd.DataFrame(orders_query.result_rows, columns=orders_query.column_names)
    quantity = orders_df.iloc[0]['quantity']
    product_id = orders_df.iloc[0]['product_id']
    store_id = orders_df.iloc[0]['store_id']
    user_id = orders_df.iloc[0]['user_id']

    pi_query = client.query(f"""
        SELECT * FROM {ch_db}.product_ingredients WHERE product_id = {product_id}
    """)
    ingredients_df = pd.DataFrame(pi_query.result_rows, columns=pi_query.column_names)
    all_available = True
    inventory_updates = []
    if ingredients_df.empty:
        all_available = False
    # Step 2: Check availability of each ingredient
    for _, pi_row in ingredients_df.iterrows():
        ingredient_id = pi_row['ingredient_id']
        qty_per_item = pi_row['qty_per_item']
        required_qty = quantity * qty_per_item

        inv_query = client.query(f"""
            SELECT * FROM {ch_db}.store_inventory FINAL 
            WHERE store_id = {store_id} AND ingredient_id = {ingredient_id}
        """)
        inv_df = pd.DataFrame(inv_query.result_rows, columns=inv_query.column_names)

        if inv_df.empty or inv_df.iloc[0]['stock_in'] < required_qty:
            all_available = False
            # client.command(f"""
            #                 INSERT INTO {ch_db}.alert_table (ingredient_id, store_id, current_stock, alert_type, updated_at)
            #                 VALUES ({ingredient_id}, {store_id}, {inv_df.iloc[0]['stock_in']}, 'Critical',now())
            #             """)
            break
        else:
            current_stock_in = inv_df.iloc[0]['stock_in']
            current_stock_out = inv_df.iloc[0]['stock_out']
            inventory_updates.append((ingredient_id, current_stock_in - required_qty,current_stock_out + required_qty))

    # Step 3: Update order and inventory
    status = 'confirmed' if all_available else 'cancelled'
    # Insert updated order status
    client.command(f"""
        INSERT INTO {ch_db}.orders (order_id,user_id,store_id, product_id, quantity, status,event_time)
        VALUES ({order_id}, {user_id}, {store_id},{product_id}, {quantity}, '{status}', now())
    """)
    # If confirmed, update inventory for all ingredients
    if all_available:
        for ingredient_id, new_stock_in,new_stock_out in inventory_updates:
            client.command(f"""
                INSERT INTO {ch_db}.store_inventory (store_id, ingredient_id, stock_in, stock_out, updated_at)
                VALUES ({store_id}, {ingredient_id}, {new_stock_in}, {new_stock_out},now())
            """)
    print(f"Order {order_id} processed: {status}")

processed_orders = set()

while True:
    orders_v_query = client.query(f"""
        SELECT * FROM {CH_DB}.mv_latest_order_status FINAL WHERE latest_status = 'created' 
        AND dateDiff('minute', last_update, now()) >= 2 and store_id = 102
    """)
    orders_v_df = pd.DataFrame(orders_v_query.result_rows, columns=orders_v_query.column_names)

    for _, row in orders_v_df.iterrows():
        order_id = row['order_id']
        # if order_id not in processed_orders:
        process_order(client, CH_DB, row)
        # processed_orders.add(order_id)
        time.sleep(3)
    # time.sleep(5)
