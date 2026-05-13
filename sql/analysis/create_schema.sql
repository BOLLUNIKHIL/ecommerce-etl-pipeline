-- ==============================================
-- STAR SCHEMA — E-Commerce Data Warehouse
-- ==============================================
-- This file creates all 5 tables of our star schema
-- Run this ONCE to set up the warehouse structure
-- ==============================================


-- Drop tables if they already exist
-- We drop in correct order because fact_sales
-- depends on the dimension tables
-- So we must drop fact first, then dimensions
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_location;


-- ==============================================
-- DIMENSION TABLE 1: dim_customer
-- Who bought?
-- ==============================================
CREATE TABLE dim_customer (
    customer_key      SERIAL PRIMARY KEY,
    customer_id       VARCHAR(255),
    customer_city     VARCHAR(255),
    customer_state    VARCHAR(10),
    customer_region   VARCHAR(50)
);


-- ==============================================
-- DIMENSION TABLE 2: dim_product
-- What was bought?
-- ==============================================
CREATE TABLE dim_product (
    product_key             SERIAL PRIMARY KEY,
    product_id              VARCHAR(255),
    product_category_name   VARCHAR(255),
    product_weight_g        FLOAT,
    weight_category         VARCHAR(50)
);


-- ==============================================
-- DIMENSION TABLE 3: dim_date
-- When was it bought?
-- ==============================================
CREATE TABLE dim_date (
    date_key        SERIAL PRIMARY KEY,
    full_date       TIMESTAMP,
    year            INTEGER,
    month           INTEGER,
    day             INTEGER,
    hour            INTEGER,
    month_name      VARCHAR(20)
);


-- ==============================================
-- DIMENSION TABLE 4: dim_location
-- Where was it shipped?
-- ==============================================
CREATE TABLE dim_location (
    location_key      SERIAL PRIMARY KEY,
    customer_city     VARCHAR(255),
    customer_state    VARCHAR(10),
    customer_region   VARCHAR(50)
);


-- ==============================================
-- FACT TABLE: fact_sales
-- The center of the star
-- One row per order item sold
-- ==============================================
CREATE TABLE fact_sales (
    sales_key           SERIAL PRIMARY KEY,
    order_id            VARCHAR(255),
    order_item_id       INTEGER,
    customer_key        INTEGER REFERENCES dim_customer(customer_key),
    product_key         INTEGER REFERENCES dim_product(product_key),
    date_key            INTEGER REFERENCES dim_date(date_key),
    location_key        INTEGER REFERENCES dim_location(location_key),
    price               FLOAT,
    freight_value       FLOAT,
    total_amount        FLOAT,
    order_status        VARCHAR(50),
    delivery_time_days  FLOAT
);