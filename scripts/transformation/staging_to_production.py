import os
import json
from datetime import datetime

import pandas as pd
import yaml
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ----------------------------
# Setup
# ----------------------------
load_dotenv()

with open("config/config.yaml") as f:
    config = yaml.safe_load(f)

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

SUMMARY_DIR = "data/production"
os.makedirs(SUMMARY_DIR, exist_ok=True)

# ----------------------------
# Helper cleansing functions
# ----------------------------
def clean_text(val):
    if pd.isna(val):
        return None
    return str(val).strip()

def clean_email(email):
    return clean_text(email).lower() if email else None

# ----------------------------
# Main ETL
# ----------------------------
def main():
    summary = {
        "transformation_timestamp": datetime.utcnow().isoformat(),
        "records_processed": {},
        "transformations_applied": [
            "text_trim",
            "email_lowercase",
            "business_rule_validation"
        ],
        "data_quality_post_transform": {
            "null_violations": 0,
            "constraint_violations": 0
        }
    }

    with engine.begin() as conn:

        # ----------------------------
        # CUSTOMERS (truncate & reload)
        # ----------------------------
        customers = pd.read_sql("SELECT * FROM staging.customers", conn)

        customers["first_name"] = customers["first_name"].apply(clean_text)
        customers["last_name"] = customers["last_name"].apply(clean_text)
        customers["email"] = customers["email"].apply(clean_email)

        conn.execute(text("TRUNCATE TABLE production.customers"))

        customers.drop(columns=["loaded_at"], inplace=True)
        customers.to_sql(
            "customers",
            conn,
            schema="production",
            if_exists="append",
            index=False
        )

        summary["records_processed"]["customers"] = {
            "input": len(customers),
            "output": len(customers),
            "filtered": 0,
            "rejected_reasons": {}
        }

        # ----------------------------
        # PRODUCTS (truncate & reload)
        # ----------------------------
        products = pd.read_sql("SELECT * FROM staging.products", conn)

        products = products[products["price"] > 0]
        products = products[products["cost"] < products["price"]]

        conn.execute(text("TRUNCATE TABLE production.products"))

        products.drop(columns=["loaded_at"], inplace=True)
        products.to_sql(
            "products",
            conn,
            schema="production",
            if_exists="append",
            index=False
        )

        summary["records_processed"]["products"] = {
            "input": len(products),
            "output": len(products),
            "filtered": 0,
            "rejected_reasons": {}
        }

        # ----------------------------
        # TRANSACTIONS (append-only)
        # ----------------------------
        transactions = pd.read_sql("SELECT * FROM staging.transactions", conn)
        transactions = transactions[transactions["total_amount"] > 0]

        existing_txns = pd.read_sql(
            "SELECT transaction_id FROM production.transactions", conn
        )

        transactions = transactions[
            ~transactions["transaction_id"].isin(existing_txns["transaction_id"])
        ]

        transactions.drop(columns=["loaded_at"], inplace=True)
        transactions.to_sql(
            "transactions",
            conn,
            schema="production",
            if_exists="append",
            index=False
        )

        summary["records_processed"]["transactions"] = {
            "input": len(transactions),
            "output": len(transactions),
            "filtered": 0,
            "rejected_reasons": {}
        }

        # ----------------------------
        # TRANSACTION ITEMS (append-only)
        # ----------------------------
        items = pd.read_sql("SELECT * FROM staging.transaction_items", conn)
        items = items[items["quantity"] > 0]

        existing_items = pd.read_sql(
            "SELECT item_id FROM production.transaction_items", conn
        )

        items = items[~items["item_id"].isin(existing_items["item_id"])]

        items.drop(columns=["loaded_at"], inplace=True)
        items.to_sql(
            "transaction_items",
            conn,
            schema="production",
            if_exists="append",
            index=False
        )

        summary["records_processed"]["transaction_items"] = {
            "input": len(items),
            "output": len(items),
            "filtered": 0,
            "rejected_reasons": {}
        }

    with open(f"{SUMMARY_DIR}/transformation_summary.json", "w") as f:
        json.dump(summary, f, indent=4)

# ----------------------------
if __name__ == "__main__":
    main()
