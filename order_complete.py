import pandas as pd
from db_setup_demo import client, CH_DB


def process_order(client, ch_db, order_row):
    order_id = order_row['order_id']
    product_id = order_row['product_id']
    quantity = order_row['quantity']
    store_id = order_row['store_id']
    user_id = order_row['user_id']
    status = 'completed'

    client.command(f"""
           INSERT INTO {ch_db}.orders (order_id,user_id, product_id, quantity, store_id, status, updated_at)
           VALUES ({order_id},{user_id}, {product_id}, {quantity}, {store_id}, '{status}', now())
       """)

    print(f"Order {order_id} processed: {status}")

orders_query = client.query(f"""
    SELECT * FROM {CH_DB}.orders FINAL WHERE status = 'confirmed'
""")
orders_df = pd.DataFrame(orders_query.result_rows, columns=orders_query.column_names)

for _, row in orders_df.iterrows():
    process_order(client,CH_DB,row)
