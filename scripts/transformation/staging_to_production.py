import yaml
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

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

# --------------------------------------------------
# ETL Logic: Staging → Production
# --------------------------------------------------
def load_customers():
    query = """
        INSERT INTO production.customers (
            customer_id, first_name, last_name, email, phone,
            registration_date, city, state, country, age_group
        )
        SELECT DISTINCT
            customer_id, first_name, last_name, email, phone,
            registration_date, city, state, country, age_group
        FROM staging.customers
        ON CONFLICT (customer_id) DO NOTHING;
    """
    return query


def load_products():
    query = """
        INSERT INTO production.products (
            product_id, product_name, category, sub_category,
            price, cost, brand, stock_quantity, supplier_id
        )
        SELECT DISTINCT
            product_id, product_name, category, sub_category,
            price, cost, brand, stock_quantity, supplier_id
        FROM staging.products
        ON CONFLICT (product_id) DO NOTHING;
    """
    return query


def load_transactions():
    query = """
        INSERT INTO production.transactions (
            transaction_id, customer_id, transaction_date,
            transaction_time, payment_method, shipping_address,
            total_amount
        )
        SELECT
            transaction_id, customer_id, transaction_date,
            transaction_time, payment_method, shipping_address,
            total_amount
        FROM staging.transactions
        ON CONFLICT (transaction_id) DO NOTHING;
    """
    return query


def load_transaction_items():
    query = """
        INSERT INTO production.transaction_items (
            item_id, transaction_id, product_id,
            quantity, unit_price, discount_percentage, line_total
        )
        SELECT
            item_id, transaction_id, product_id,
            quantity, unit_price, discount_percentage, line_total
        FROM staging.transaction_items
        ON CONFLICT (item_id) DO NOTHING;
    """
    return query


# --------------------------------------------------
# Execute ETL
# --------------------------------------------------
if __name__ == "__main__":
    with engine.begin() as conn:
        conn.execute(text(load_customers()))
        conn.execute(text(load_products()))
        conn.execute(text(load_transactions()))
        conn.execute(text(load_transaction_items()))

    print("Staging to Production ETL completed successfully.")
