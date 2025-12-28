import os
import pandas as pd

def test_csv_files_exist():
    files = [
        "data/raw/customers.csv",
        "data/raw/products.csv",
        "data/raw/transactions.csv",
        "data/raw/transaction_items.csv"
    ]

    for file in files:
        assert os.path.exists(file), f"{file} does not exist"


def test_customers_columns():
    df = pd.read_csv("data/raw/customers.csv")
    required_cols = {"customer_id", "first_name", "last_name", "email"}

    assert required_cols.issubset(df.columns), \
        f"Missing required columns in customers. Found: {df.columns.tolist()}"

def test_no_null_customer_ids():
    df = pd.read_csv("data/raw/customers.csv")
    assert df["customer_id"].isnull().sum() == 0, "Null customer_id found"


def test_transaction_items_line_total():
    df = pd.read_csv("data/raw/transaction_items.csv")

    row = df.iloc[0]
    calculated = row["quantity"] * row["unit_price"]

    assert abs(calculated - row["line_total"]) < 0.01, "Incorrect line_total calculation"
