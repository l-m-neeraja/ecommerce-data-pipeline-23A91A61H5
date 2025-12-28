import yaml
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import date

# --------------------------------------------------
# Load environment variables
# --------------------------------------------------
load_dotenv()

# --------------------------------------------------
# Load configuration
# --------------------------------------------------
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

# --------------------------------------------------
# Database connection
# --------------------------------------------------
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

TODAY = date.today()

# --------------------------------------------------
# Load Date Dimension
# --------------------------------------------------
def load_dim_date():
    query = """
        INSERT INTO warehouse.dim_date (
            date_key, full_date, day, month, month_name,
            quarter, year, day_of_week, day_name
        )
        SELECT DISTINCT
            TO_CHAR(transaction_date, 'YYYYMMDD')::INTEGER AS date_key,
            transaction_date AS full_date,
            EXTRACT(DAY FROM transaction_date),
            EXTRACT(MONTH FROM transaction_date),
            TO_CHAR(transaction_date, 'Month'),
            EXTRACT(QUARTER FROM transaction_date),
            EXTRACT(YEAR FROM transaction_date),
            EXTRACT(DOW FROM transaction_date),
            TO_CHAR(transaction_date, 'Day')
        FROM production.transactions
        ON CONFLICT (date_key) DO NOTHING;
    """
    return query

# --------------------------------------------------
# Load Customers Dimension (SCD Type 2)
# --------------------------------------------------
def load_dim_customers():
    query = """
        INSERT INTO warehouse.dim_customers (
            customer_id, first_name, last_name, email,
            phone, city, state, country, age_group,
            effective_date, end_date, is_current
        )
        SELECT
            c.customer_id, c.first_name, c.last_name, c.email,
            c.phone, c.city, c.state, c.country, c.age_group,
            CURRENT_DATE, NULL, TRUE
        FROM production.customers c
        WHERE NOT EXISTS (
            SELECT 1
            FROM warehouse.dim_customers d
            WHERE d.customer_id = c.customer_id
              AND d.is_current = TRUE
        );
    """
    return query

# --------------------------------------------------
# Load Products Dimension (SCD Type 2)
# --------------------------------------------------
def load_dim_products():
    query = """
        INSERT INTO warehouse.dim_products (
            product_id, product_name, category, sub_category,
            brand, price, cost,
            effective_date, end_date, is_current
        )
        SELECT
            p.product_id, p.product_name, p.category, p.sub_category,
            p.brand, p.price, p.cost,
            CURRENT_DATE, NULL, TRUE
        FROM production.products p
        WHERE NOT EXISTS (
            SELECT 1
            FROM warehouse.dim_products d
            WHERE d.product_id = p.product_id
              AND d.is_current = TRUE
        );
    """
    return query

# --------------------------------------------------
# Load Fact Sales (Line Item Grain)
# --------------------------------------------------
def load_fact_sales():
    query = """
        INSERT INTO warehouse.fact_sales (
            transaction_id, item_id,
            customer_key, product_key, date_key,
            quantity, unit_price, discount_amount, total_amount
        )
        SELECT
            ti.transaction_id,
            ti.item_id,
            dc.customer_key,
            dp.product_key,
            dd.date_key,
            ti.quantity,
            ti.unit_price,
            (ti.unit_price * ti.quantity * ti.discount_percentage / 100),
            ti.line_total
        FROM production.transaction_items ti
        JOIN production.transactions t
            ON ti.transaction_id = t.transaction_id
        JOIN warehouse.dim_customers dc
            ON dc.customer_id = t.customer_id
           AND dc.is_current = TRUE
        JOIN warehouse.dim_products dp
            ON dp.product_id = ti.product_id
           AND dp.is_current = TRUE
        JOIN warehouse.dim_date dd
            ON dd.full_date = t.transaction_date;
    """
    return query

# --------------------------------------------------
# Execute Warehouse Load
# --------------------------------------------------
if __name__ == "__main__":
    with engine.begin() as conn:
        conn.execute(text(load_dim_date()))
        conn.execute(text(load_dim_customers()))
        conn.execute(text(load_dim_products()))
        conn.execute(text(load_fact_sales()))

    print("Warehouse load completed successfully.")
