import json
import random
import time
import datetime
import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import clickhouse_connect as conn
import config
from db_setup_demo import client

# --- Load supporting data from ClickHouse ---
CH_DB = "PIZZA"
# client = conn.get_client(
#     host='cg9shztluj.asia-southeast1.gcp.clickhouse.cloud',
#     port=8443,
#     user='default',
#     password='u3bV~fmYNOBxg'
# )
# client=conn.get_client(host='cg9shztluj.asia-southeast1.gcp.clickhouse.cloud',
#                                   port=8443,
#                                   user='default',
#                                   password='u3bV~fmYNOBxg')

users = pd.DataFrame(client.query(f"SELECT user_id FROM {CH_DB}.users").result_rows, columns=["user_id"])
products = pd.DataFrame(client.query(f"SELECT product_id FROM {CH_DB}.products").result_rows, columns=["product_id"])
stores = pd.DataFrame(client.query(f"SELECT store_id FROM {CH_DB}.stores").result_rows, columns=["store_id"])

user_ids = users["user_id"].tolist()
product_ids = products["product_id"].tolist()
store_ids = stores["store_id"].tolist()

# # --- Kafka Producer Config ---
# conf = {
#     'bootstrap.servers': "d3dlu91t7088ps7bl8q0.any.us-east-1.mpx.prd.cloud.redpanda.com:9092",
#     'security.protocol': "SASL_SSL",
#     'sasl.mechanisms': "SCRAM-SHA-256",
#     'sasl.username': "connect",
#     'sasl.password': "FMNAx1dax7con2AWC6EmAkHkNup5FC",
# }

producer = Producer(config.KAFKA_CONF)
admin_client = AdminClient(config.KAFKA_CONF)

def create_topic(topic_name, num_partitions=1, replication_factor=1):
    """Try to create a Kafka topic and log results"""
    print(f"📌 Attempting to create topic: {topic_name}")
    topic_list = [NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]
    fs = admin_client.create_topics(topic_list)

    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None if successful
            print(f"✅ Successfully created topic: {topic}")
        except Exception as e:
            if "TopicExistsError" in str(e):
                print(f"ℹ️ Topic {topic} already exists")
            else:
                print(f"❌ Failed to create topic {topic}: {e}")

def get_last_order_id():
    try:
        result = client.query(f"SELECT max(order_id) FROM {CH_DB}.orders").result_rows
        last_id = result[0][0] if result and result[0][0] is not None else 0
        print(f"📌 Last order_id in ClickHouse = {last_id}")
        return last_id
    except Exception as e:
        print(f"⚠️ Could not fetch last order_id: {e}")
        return 0

last_order_id = get_last_order_id()
order_counter = last_order_id + 1
# --- Order Generator ---
def generate_order(order_id):
    product_id = random.choice(product_ids)
    user_id = random.choice(user_ids)
    store_id = random.choice(store_ids)
    qty = random.randint(1, 5)
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    return {
        "order_id":order_counter,
        "user_id": user_id,
        "store_id": store_id,
        "product_id": product_id,
        "quantity": qty,
        "status": "initiated",
         "created_at": now
    }

create_topic("demo-topic", num_partitions=1, replication_factor=3)

# --- Publish Orders to Kafka ---
order_counter = 1
while True:
    order = generate_order(order_counter)
    msg = json.dumps(order)
    try:
        print(f"📤 Producing order {order_counter} to topic `demo-topic` ...")
        producer.produce("demo-topic", key=str(order_counter), value=msg)
        producer.flush()
        print(f"✅ Sent order {order_counter} -> Redpanda topic `demo-topic`")
    except Exception as e:
        print(f"❌ Failed to send order {order_counter}: {e}")
    order_counter += 1
    time.sleep(2)   # send one order every 5 seconds
