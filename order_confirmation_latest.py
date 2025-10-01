import pandas as pd
from db_setup_demo import client, CH_DB


def process_order(client, ch_db, order_row):
    order_id = order_row['order_id']
    product_id = order_row['product_id']
    quantity = order_row['quantity']
    store_id = order_row['store_id']
    user_id = order_row['user_id']
    created_at = pd.to_datetime(order_row['created_at']).strftime('%Y-%m-%d %H:%M:%S')

    # Step 1: Get all ingredients for the product
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
            break
        else:
            current_stock_in = inv_df.iloc[0]['stock_in']
            current_stock_out = inv_df.iloc[0]['stock_out']
            inventory_updates.append((ingredient_id, current_stock_in - required_qty,current_stock_out + required_qty))

    # Step 3: Update order and inventory
    status = 'confirmed' if all_available else 'cancelled'
    # Insert updated order status
    client.command(f"""
        INSERT INTO {ch_db}.orders (order_id,user_id, product_id, quantity, store_id, status,created_at, updated_at)
        VALUES ({order_id}, {user_id}, {product_id}, {quantity}, {store_id}, '{status}', '{created_at}', now())
    """)
    # If confirmed, update inventory for all ingredients
    if all_available:
        for ingredient_id, new_stock_in,new_stock_out in inventory_updates:
            client.command(f"""
                INSERT INTO {ch_db}.store_inventory (store_id, ingredient_id, stock_in, stock_out, updated_at)
                VALUES ({store_id}, {ingredient_id}, {new_stock_in}, {new_stock_out},now())
            """)
    print(f"Order {order_id} processed: {status}")

orders_query = client.query(f"""
    SELECT * FROM {CH_DB}.orders FINAL WHERE status = 'initiated'
""")
orders_df = pd.DataFrame(orders_query.result_rows, columns=orders_query.column_names)

for _, row in orders_df.iterrows():
    process_order(client,CH_DB,row)
