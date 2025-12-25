import os
import json
import random
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker
import yaml

# ----------------------------
# Load configuration
# ----------------------------
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

DATA_DIR = "data/raw"
os.makedirs(DATA_DIR, exist_ok=True)

fake = Faker()

# ----------------------------
# Helper functions
# ----------------------------
def generate_id(prefix, number, pad):
    return f"{prefix}{number:0{pad}d}"

# ----------------------------
# Generate Customers
# ----------------------------
def generate_customers():
    customers = []
    used_emails = set()

    for i in range(1, config["data_generation"]["customers_count"] + 1):
        email = fake.email()
        while email in used_emails:
            email = fake.email()
        used_emails.add(email)

        customers.append({
            "customer_id": generate_id("CUST", i, 4),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": email,
            "phone": fake.phone_number(),
            "registration_date": fake.date_between(start_date="-3y", end_date="today"),
            "city": fake.city(),
            "state": fake.state(),
            "country": fake.country(),
            "age_group": random.choice(["18-25", "26-35", "36-45", "46-60", "60+"])
        })

    return pd.DataFrame(customers)

# ----------------------------
# Generate Products
# ----------------------------
def generate_products():
    categories = ["Electronics", "Clothing", "Home & Kitchen", "Books", "Sports", "Beauty"]
    products = []

    for i in range(1, config["data_generation"]["products_count"] + 1):
        price = round(random.uniform(100, 5000), 2)
        cost = round(price * random.uniform(0.5, 0.8), 2)

        products.append({
            "product_id": generate_id("PROD", i, 4),
            "product_name": fake.word().title(),
            "category": random.choice(categories),
            "sub_category": fake.word().title(),
            "price": price,
            "cost": cost,
            "brand": fake.company(),
            "stock_quantity": random.randint(10, 500),
            "supplier_id": f"SUP{random.randint(1, 50):03d}"
        })

    return pd.DataFrame(products)

# ----------------------------
# Generate Transactions & Items
# ----------------------------
def generate_transactions(customers_df, products_df):
    transactions = []
    items = []

    for i in range(1, config["data_generation"]["transactions_count"] + 1):
        transaction_id = generate_id("TXN", i, 5)
        customer_id = random.choice(customers_df["customer_id"].tolist())
        transaction_date = fake.date_between(
            start_date=config["data_generation"]["start_date"],
            end_date=config["data_generation"]["end_date"]
        )

        total_amount = 0
        num_items = random.randint(1, 5)

        for j in range(num_items):
            product = products_df.sample(1).iloc[0]
            quantity = random.randint(1, 5)
            discount = random.choice([0, 5, 10, 15])
            line_total = round(
                quantity * product["price"] * (1 - discount / 100), 2
            )
            total_amount += line_total

            items.append({
                "item_id": generate_id("ITEM", len(items) + 1, 5),
                "transaction_id": transaction_id,
                "product_id": product["product_id"],
                "quantity": quantity,
                "unit_price": product["price"],
                "discount_percentage": discount,
                "line_total": line_total
            })

        transactions.append({
            "transaction_id": transaction_id,
            "customer_id": customer_id,
            "transaction_date": transaction_date,
            "transaction_time": fake.time(),
            "payment_method": random.choice(
                ["Credit Card", "Debit Card", "UPI", "Cash on Delivery", "Net Banking"]
            ),
            "shipping_address": fake.address(),
            "total_amount": round(total_amount, 2)
        })

    return pd.DataFrame(transactions), pd.DataFrame(items)

# ----------------------------
# Validation
# ----------------------------
def validate_referential_integrity(customers, products, transactions, items):
    issues = {
        "orphan_customers": 0,
        "orphan_products": 0,
        "orphan_transactions": 0
    }

    issues["orphan_customers"] = (
        ~transactions["customer_id"].isin(customers["customer_id"])
    ).sum()

    issues["orphan_products"] = (
        ~items["product_id"].isin(products["product_id"])
    ).sum()

    issues["orphan_transactions"] = (
        ~items["transaction_id"].isin(transactions["transaction_id"])
    ).sum()

    score = 100 if sum(issues.values()) == 0 else 90

    return {
        "issues": issues,
        "data_quality_score": score
    }

# ----------------------------
# Main execution
# ----------------------------
def main():
    customers = generate_customers()
    products = generate_products()
    transactions, items = generate_transactions(customers, products)

    customers.to_csv(f"{DATA_DIR}/customers.csv", index=False)
    products.to_csv(f"{DATA_DIR}/products.csv", index=False)
    transactions.to_csv(f"{DATA_DIR}/transactions.csv", index=False)
    items.to_csv(f"{DATA_DIR}/transaction_items.csv", index=False)

    validation = validate_referential_integrity(
        customers, products, transactions, items
    )

    metadata = {
        "generated_at": datetime.utcnow().isoformat(),
        "record_counts": {
            "customers": len(customers),
            "products": len(products),
            "transactions": len(transactions),
            "transaction_items": len(items)
        },
        "date_range": {
            "start": str(transactions["transaction_date"].min()),
            "end": str(transactions["transaction_date"].max())
        },
        "validation": validation
    }

    with open(f"{DATA_DIR}/generation_metadata.json", "w") as f:
        json.dump(metadata, f, indent=4)

if __name__ == "__main__":
    main()
