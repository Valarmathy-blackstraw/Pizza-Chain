CREATE DATABASE IF NOT EXISTS PIZZA;

--Users Table
CREATE TABLE IF NOT EXISTS PIZZA.users
    (
      user_id UInt64,
      first_name String,
      last_name String,
      email String,
      residence String,
      lat Float64,
      lon Float64
    ) ENGINE = MergeTree() ORDER BY user_id;

-- Products Table
CREATE TABLE IF NOT EXISTS PIZZA.products
    (
      product_id UInt32,
      name String,
      description String,
      price Float32,
      category LowCardinality(String),
      image String
    ) ENGINE = MergeTree() ORDER BY product_id;

-- Stores Table
CREATE TABLE IF NOT EXISTS PIZZA.stores
    (
      store_id UInt32,
      store_name String,
      city String
    ) ENGINE = MergeTree() ORDER BY store_id;

-- Ingredients table
CREATE TABLE IF NOT EXISTS PIZZA.ingredients
    (
      ingredient_id UInt32,
      ingredient_name String,
      unit String
    ) ENGINE = MergeTree() ORDER BY ingredient_id;

-- Product Ingredients table
CREATE TABLE IF NOT EXISTS PIZZA.product_ingredients
    (
      product_id UInt32,
      ingredient_id UInt32,
      qty_per_item Float32
    ) ENGINE = MergeTree() ORDER BY (product_id, ingredient_id);

-- Store Inventory Table
CREATE TABLE IF NOT EXISTS PIZZA.store_inventory
    (
    store_id UInt32,
    ingredient_id UInt32,
    stock_in UInt32,
    stock_out UInt32,
    updated_at DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (store_id, ingredient_id);

-- Orders Table
CREATE TABLE IF NOT EXISTS PIZZA.orders
    (
      order_id UInt64,
      user_id UInt64,
      store_id UInt32,
      product_id UInt32,
      quantity UInt32,
      status Enum8('created' = 1, 'confirmed' = 2, 'completed' = 3, 'cancelled' = 4) DEFAULT 'created',
      event_time DateTime,
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(event_time)
    ORDER BY (order_id,event_time);

CREATE TABLE PIZZA.latest_order_status
(
    order_id      UInt64,
    store_id UInt32,
    latest_status Enum8('created' = 1, 'confirmed' = 2, 'completed' = 3, 'cancelled' = 4),
    last_update   DateTime
)
ENGINE = ReplacingMergeTree(last_update)
ORDER BY order_id;

CREATE MATERIALIZED VIEW PIZZA.mv_latest_order_status
TO PIZZA.latest_order_status
AS
SELECT
    order_id,
    store_id,
    argMax(status, event_time) AS latest_status,
    max(event_time) AS last_update
FROM PIZZA.orders
GROUP BY order_id,store_id;

ALTER TABLE PIZZA.orders
ADD PROJECTION product_processing_duration_proj
(
    SELECT
        product_id,
        order_id,
        dateDiff(
            'second',
            MINIf(event_time, status = 'created'),
            MAXIf(event_time, status = 'completed')
        ) AS processing_duration_seconds
    GROUP BY product_id, order_id
);
ALTER TABLE PIZZA.orders MATERIALIZE PROJECTION product_processing_duration_proj;

