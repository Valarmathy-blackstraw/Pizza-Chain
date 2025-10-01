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
      category String,
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
    updated_at DateTime
    ) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (store_id, ingredient_id);

-- Orders Table
CREATE TABLE IF NOT EXISTS PIZZA.orders
    (
      order_id UInt64,
      user_id UInt64,
      store_id UInt32,
      product_id UInt32,
      quantity UInt32,
      status String,
      created_at DateTime,
      updated_at DateTime,
      confirmed_at Nullable(DateTime),
      completed_at Nullable(DateTime)
    ) ENGINE = ReplacingMergeTree(updated_at)
    PARTITION BY toYYYYMM(created_at)
    ORDER BY (order_id);

