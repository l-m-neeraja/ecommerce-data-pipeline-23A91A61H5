import pandas as pd
import random
import yaml
from faker import Faker
from datetime import datetime

fake = Faker()

# --------------------------------------------------
# Load configuration
# --------------------------------------------------
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

dg = config["data_generation"]

START_DATE = datetime.strptime(dg["start_date"], "%Y-%m-%d").date()
END_DATE = datetime.strptime(dg["end_date"], "%Y-%m-%d").date()

# --------------------------------------------------
# Generate Customers
# --------------------------------------------------
def generate_customers(num_customers: int) -> pd.DataFrame:
    customers = []

    for i in range(1, num_customers + 1):
        customers.append({
            "customer_id": f"CUST{i:05d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.unique.email().lower(),
            "phone": fake.phone_number(),
            "registration_date": fake.date_between(
                start_date=START_DATE,
                end_date=END_DATE
            ),
            "city": fake.city(),
            "state": fake.state(),
            "country": "India",
            "age_group": random.choice(["18-25", "26-35", "36-45", "46-60"])
        })

    return pd.DataFrame(customers)

# --------------------------------------------------
# Generate Products
# --------------------------------------------------
def generate_products(num_products: int) -> pd.DataFrame:
    categories = ["Electronics", "Clothing", "Home & Kitchen", "Books", "Sports", "Beauty"]
    products = []

    for i in range(1, num_products + 1):
        price = round(random.uniform(100, 5000), 2)
        cost = round(price * random.uniform(0.5, 0.8), 2)

        products.append({
            "product_id": f"PROD{i:05d}",
            "product_name": fake.word().title(),
            "category": random.choice(categories),
            "sub_category": fake.word().title(),
            "price": price,
            "cost": cost,
            "brand": fake.company(),
            "stock_quantity": random.randint(10, 500),
            "supplier_id": f"SUP{random.randint(1, 100):03d}"
        })

    return pd.DataFrame(products)

# --------------------------------------------------
# Generate Transactions
# --------------------------------------------------
def generate_transactions(customers_df: pd.DataFrame, num_transactions: int) -> pd.DataFrame:
    payment_methods = ["Credit Card", "Debit Card", "UPI", "Cash on Delivery", "Net Banking"]
    transactions = []

    for i in range(1, num_transactions + 1):
        transactions.append({
            "transaction_id": f"TXN{i:05d}",
            "customer_id": random.choice(customers_df["customer_id"].tolist()),
            "transaction_date": fake.date_between(
                start_date=START_DATE,
                end_date=END_DATE
            ),
            "transaction_time": fake.time(),
            "payment_method": random.choice(payment_methods),
            "shipping_address": fake.address().replace("\n", ", "),
            "total_amount": 0.0
        })

    return pd.DataFrame(transactions)

# --------------------------------------------------
# Generate Transaction Items
# --------------------------------------------------
def generate_transaction_items(
    transactions_df: pd.DataFrame,
    products_df: pd.DataFrame
) -> pd.DataFrame:

    items = []
    item_id = 1

    for _, txn in transactions_df.iterrows():
        num_items = random.randint(
            dg["min_items_per_transaction"],
            dg["max_items_per_transaction"]
        )

        for _ in range(num_items):
            product = products_df.sample(1).iloc[0]
            quantity = random.randint(1, 5)
            discount = random.choice([0, 5, 10, 15, 20])

            line_total = round(
                quantity * product["price"] * (1 - discount / 100), 2
            )

            items.append({
                "item_id": f"ITEM{item_id:06d}",
                "transaction_id": txn["transaction_id"],
                "product_id": product["product_id"],
                "quantity": quantity,
                "unit_price": product["price"],
                "discount_percentage": discount,
                "line_total": line_total
            })

            item_id += 1

    return pd.DataFrame(items)

# --------------------------------------------------
# Execute Generation
# --------------------------------------------------
if __name__ == "__main__":
    customers_df = generate_customers(dg["customers"])
    products_df = generate_products(dg["products"])
    transactions_df = generate_transactions(customers_df, dg["transactions"])
    items_df = generate_transaction_items(transactions_df, products_df)

    customers_df.to_csv("data/raw/customers.csv", index=False)
    products_df.to_csv("data/raw/products.csv", index=False)
    transactions_df.to_csv("data/raw/transactions.csv", index=False)
    items_df.to_csv("data/raw/transaction_items.csv", index=False)
