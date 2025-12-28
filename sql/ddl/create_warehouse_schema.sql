-- ============================================================
-- Warehouse Schema
-- Purpose:
--   Dimensional model (Star Schema)
--   Supports analytics and BI
--   Implements SCD Type 2 for dimensions
-- ============================================================

CREATE SCHEMA IF NOT EXISTS warehouse;

-- ----------------------------
-- Dimension: Customers (SCD Type 2)
-- ----------------------------
DROP TABLE IF EXISTS warehouse.dim_customers;

CREATE TABLE warehouse.dim_customers (
    customer_key      SERIAL PRIMARY KEY,
    customer_id       VARCHAR(20),
    first_name        VARCHAR(50),
    last_name         VARCHAR(50),
    email             VARCHAR(100),
    phone             VARCHAR(50),
    city              VARCHAR(50),
    state             VARCHAR(50),
    country           VARCHAR(50),
    age_group         VARCHAR(20),
    effective_date    DATE,
    end_date          DATE,
    is_current        BOOLEAN DEFAULT TRUE
);

-- ----------------------------
-- Dimension: Products (SCD Type 2)
-- ----------------------------
DROP TABLE IF EXISTS warehouse.dim_products;

CREATE TABLE warehouse.dim_products (
    product_key       SERIAL PRIMARY KEY,
    product_id        VARCHAR(20),
    product_name      VARCHAR(100),
    category          VARCHAR(50),
    sub_category      VARCHAR(50),
    brand             VARCHAR(100),
    price             DECIMAL(10,2),
    cost              DECIMAL(10,2),
    effective_date    DATE,
    end_date          DATE,
    is_current        BOOLEAN DEFAULT TRUE
);

-- ----------------------------
-- Dimension: Date
-- ----------------------------
DROP TABLE IF EXISTS warehouse.dim_date;

CREATE TABLE warehouse.dim_date (
    date_key        INTEGER PRIMARY KEY,
    full_date       DATE,
    day             INTEGER,
    month           INTEGER,
    month_name      VARCHAR(20),
    quarter         INTEGER,
    year            INTEGER,
    day_of_week     INTEGER,
    day_name        VARCHAR(20)
);

-- ----------------------------
-- Fact: Sales (Line Item Grain)
-- ----------------------------
DROP TABLE IF EXISTS warehouse.fact_sales;

CREATE TABLE warehouse.fact_sales (
    sales_key        SERIAL PRIMARY KEY,
    transaction_id  VARCHAR(20),
    item_id         VARCHAR(20),
    customer_key    INTEGER,
    product_key     INTEGER,
    date_key        INTEGER,
    quantity        INTEGER,
    unit_price      DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    total_amount    DECIMAL(12,2),
    CONSTRAINT fk_sales_customer
        FOREIGN KEY (customer_key)
        REFERENCES warehouse.dim_customers (customer_key),
    CONSTRAINT fk_sales_product
        FOREIGN KEY (product_key)
        REFERENCES warehouse.dim_products (product_key),
    CONSTRAINT fk_sales_date
        FOREIGN KEY (date_key)
        REFERENCES warehouse.dim_date (date_key)
);
