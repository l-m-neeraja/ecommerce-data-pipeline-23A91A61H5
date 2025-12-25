import os
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ----------------------------
# Setup
# ----------------------------
load_dotenv()

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

# ----------------------------
# DIM DATE
# ----------------------------
def build_dim_date(start_date, end_date, conn):
    dates = []
    current = start_date

    while current <= end_date:
        dates.append({
            "date_key": int(current.strftime("%Y%m%d")),
            "full_date": current,
            "year": current.year,
            "quarter": (current.month - 1)//3 + 1,
            "month": current.month,
            "day": current.day,
            "month_name": current.strftime("%B"),
            "day_name": current.strftime("%A"),
            "week_of_year": current.isocalendar()[1],
            "is_weekend": current.weekday() >= 5
        })
        current += timedelta(days=1)

    df = pd.DataFrame(dates)
    conn.execute(text("TRUNCATE warehouse.dim_date"))
    df.to_sql("dim_date", conn, schema="warehouse", if_exists="append", index=False)

# ----------------------------
# MAIN LOAD
# ----------------------------
def main():
    with engine.begin() as conn:

        # ----------------------------
        # DIM DATE
        # ----------------------------
        build_dim_date(
            datetime(2024, 1, 1).date(),
            datetime(2024, 12, 31).date(),
            conn
        )

        # ----------------------------
        # DIM PAYMENT METHOD
        # ----------------------------
        payment_methods = pd.read_sql(
            "SELECT DISTINCT payment_method FROM production.transactions", conn
        )

        payment_methods["payment_type"] = payment_methods["payment_method"].apply(
            lambda x: "Offline" if x == "Cash on Delivery" else "Online"
        )

        conn.execute(text("TRUNCATE warehouse.dim_payment_method"))
        payment_methods.rename(
            columns={"payment_method": "payment_method_name"}, inplace=True
        )

        payment_methods.to_sql(
            "dim_payment_method",
            conn,
            schema="warehouse",
            if_exists="append",
            index=False
        )

        # ----------------------------
        # DIM CUSTOMERS (SCD TYPE 2 - BASIC)
        # ----------------------------
        customers = pd.read_sql(
            "SELECT * FROM production.customers", conn
        )

        conn.execute(text("TRUNCATE warehouse.dim_customers"))

        customers["full_name"] = customers["first_name"] + " " + customers["last_name"]
        customers["effective_date"] = datetime.utcnow().date()
        customers["end_date"] = None
        customers["is_current"] = True

        customers = customers[[
            "customer_id", "full_name", "email",
            "city", "state", "country", "age_group",
            "registration_date", "effective_date",
            "end_date", "is_current"
        ]]

        customers.to_sql(
            "dim_customers",
            conn,
            schema="warehouse",
            if_exists="append",
            index=False
        )

        # ----------------------------
        # DIM PRODUCTS (SCD TYPE 2 - BASIC)
        # ----------------------------
        products = pd.read_sql(
            "SELECT * FROM production.products", conn
        )

        def price_range(price):
            if price < 50:
                return "Budget"
            elif price < 200:
                return "Mid-range"
            return "Premium"

        products["price_range"] = products["price"].apply(price_range)
        products["effective_date"] = datetime.utcnow().date()
        products["end_date"] = None
        products["is_current"] = True

        products = products[[
            "product_id", "product_name", "category",
            "sub_category", "brand", "price_range",
            "effective_date", "end_date", "is_current"
        ]]

        conn.execute(text("TRUNCATE warehouse.dim_products"))

        products.to_sql(
            "dim_products",
            conn,
            schema="warehouse",
            if_exists="append",
            index=False
        )

        # ----------------------------
        # FACT SALES
        # ----------------------------
        fact = pd.read_sql("""
            SELECT
                ti.transaction_id,
                ti.quantity,
                ti.unit_price,
                ti.discount_percentage,
                ti.line_total,
                (ti.line_total - (p.cost * ti.quantity)) AS profit,
                t.transaction_date,
                t.payment_method,
                t.customer_id,
                ti.product_id
            FROM production.transaction_items ti
            JOIN production.transactions t
              ON ti.transaction_id = t.transaction_id
            JOIN production.products p
              ON ti.product_id = p.product_id
        """, conn)

        fact["date_key"] = fact["transaction_date"].apply(
            lambda d: int(d.strftime("%Y%m%d"))
        )

        fact["discount_amount"] = (
            fact["unit_price"] * fact["quantity"] *
            (fact["discount_percentage"] / 100)
        )

        fact = fact.merge(
            pd.read_sql(
                "SELECT customer_key, customer_id FROM warehouse.dim_customers "
                "WHERE is_current = TRUE", conn
            ),
            on="customer_id"
        )

        fact = fact.merge(
            pd.read_sql(
                "SELECT product_key, product_id FROM warehouse.dim_products "
                "WHERE is_current = TRUE", conn
            ),
            on="product_id"
        )

        fact = fact.merge(
            pd.read_sql(
                "SELECT payment_method_key, payment_method_name "
                "FROM warehouse.dim_payment_method", conn
            ),
            left_on="payment_method",
            right_on="payment_method_name"
        )

        fact = fact[[
            "date_key", "customer_key", "product_key",
            "payment_method_key", "transaction_id",
            "quantity", "unit_price",
            "discount_amount", "line_total", "profit"
        ]]

        conn.execute(text("TRUNCATE warehouse.fact_sales"))

        fact.to_sql(
            "fact_sales",
            conn,
            schema="warehouse",
            if_exists="append",
            index=False
        )

        # ----------------------------
        # AGGREGATES
        # ----------------------------
        conn.execute(text("TRUNCATE warehouse.agg_daily_sales"))
        conn.execute(text("""
            INSERT INTO warehouse.agg_daily_sales
            SELECT
                date_key,
                COUNT(DISTINCT transaction_id),
                SUM(line_total),
                SUM(profit),
                COUNT(DISTINCT customer_key)
            FROM warehouse.fact_sales
            GROUP BY date_key
        """))

# ----------------------------
if __name__ == "__main__":
    main()
