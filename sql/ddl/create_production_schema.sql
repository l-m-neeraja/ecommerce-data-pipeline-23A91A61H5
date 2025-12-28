-- ============================================================
-- Production Schema
-- Purpose:
--   Cleaned, validated, relational tables
--   Enforces primary keys and foreign keys
-- ============================================================

CREATE SCHEMA IF NOT EXISTS production;

-- ----------------------------
-- Customers (Production)
-- ----------------------------
DROP TABLE IF EXISTS production.customers;

CREATE TABLE production.customers (
    customer_id        VARCHAR(20) PRIMARY KEY,
    first_name         VARCHAR(50),
    last_name          VARCHAR(50),
    email              VARCHAR(100),
    phone              VARCHAR(50),
    registration_date  DATE,
    city               VARCHAR(50),
    state              VARCHAR(50),
    country            VARCHAR(50),
    age_group          VARCHAR(20),
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------
-- Products (Production)
-- ----------------------------
DROP TABLE IF EXISTS production.products;

CREATE TABLE production.products (
    product_id       VARCHAR(20) PRIMARY KEY,
    product_name     VARCHAR(100),
    category         VARCHAR(50),
    sub_category     VARCHAR(50),
    price            DECIMAL(10,2),
    cost             DECIMAL(10,2),
    brand            VARCHAR(100),
    stock_quantity   INTEGER,
    supplier_id      VARCHAR(50),
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------
-- Transactions (Production)
-- ----------------------------
DROP TABLE IF EXISTS production.transactions;

CREATE TABLE production.transactions (
    transaction_id     VARCHAR(20) PRIMARY KEY,
    customer_id        VARCHAR(20),
    transaction_date   DATE,
    transaction_time   TIME,
    payment_method     VARCHAR(50),
    shipping_address   TEXT,
    total_amount       DECIMAL(12,2),
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_transactions_customer
        FOREIGN KEY (customer_id)
        REFERENCES production.customers (customer_id)
);

-- ----------------------------
-- Transaction Items (Production)
-- ----------------------------
DROP TABLE IF EXISTS production.transaction_items;

CREATE TABLE production.transaction_items (
    item_id               VARCHAR(20) PRIMARY KEY,
    transaction_id        VARCHAR(20),
    product_id            VARCHAR(20),
    quantity              INTEGER,
    unit_price            DECIMAL(10,2),
    discount_percentage   DECIMAL(5,2),
    line_total            DECIMAL(12,2),
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_items_transaction
        FOREIGN KEY (transaction_id)
        REFERENCES production.transactions (transaction_id),
    CONSTRAINT fk_items_product
        FOREIGN KEY (product_id)
        REFERENCES production.products (product_id)
);
