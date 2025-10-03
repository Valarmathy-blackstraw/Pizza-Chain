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
    status = 'completed'

    client.command(f"""
        INSERT INTO {ch_db}.orders (order_id,user_id,store_id, product_id, quantity, status,event_time)
        VALUES ({order_id}, {user_id}, {store_id},{product_id}, {quantity}, '{status}', now())
    """)
    print(f"Order {order_id} processed: {status}")

orders_v_query = client.query(f"""
    SELECT * FROM {CH_DB}.mv_latest_order_status FINAL WHERE latest_status = 'confirmed'
""")
orders_v_df = pd.DataFrame(orders_v_query.result_rows, columns=orders_v_query.column_names)

for _, row in orders_v_df.iterrows():
    process_order(client,CH_DB,row)
