import pandas as pd
import sys

# --------------------------------------------------
# Data Quality Checks
# --------------------------------------------------
def check_nulls(df: pd.DataFrame, table_name: str, allowed_nulls=None):
    if allowed_nulls is None:
        allowed_nulls = []

    null_counts = df.isnull().sum()
    null_violations = null_counts.drop(labels=allowed_nulls, errors="ignore")

    if null_violations.any():
        print(f"[FAIL] Null values found in {table_name}")
        print(null_violations[null_violations > 0])
        sys.exit(1)

    print(f"[PASS] No invalid nulls in {table_name}")


def check_duplicates(df: pd.DataFrame, key_column: str, table_name: str):
    duplicate_count = df.duplicated(subset=[key_column]).sum()
    if duplicate_count > 0:
        print(f"[FAIL] Duplicate {key_column} found in {table_name}")
        sys.exit(1)
    print(f"[PASS] No duplicates in {table_name}")


def check_positive_values(df: pd.DataFrame, column: str, table_name: str):
    if (df[column] < 0).any():
        print(f"[FAIL] Negative values found in {table_name}.{column}")
        sys.exit(1)
    print(f"[PASS] All values positive in {table_name}.{column}")


# --------------------------------------------------
# Execute Quality Checks on Raw Data
# --------------------------------------------------
if __name__ == "__main__":
    customers = pd.read_csv("data/raw/customers.csv")
    products = pd.read_csv("data/raw/products.csv")
    transactions = pd.read_csv("data/raw/transactions.csv")
    items = pd.read_csv("data/raw/transaction_items.csv")

    # Customers
    check_nulls(customers, "customers")
    check_duplicates(customers, "customer_id", "customers")

    # Products
    check_nulls(
        products,
        "products",
        allowed_nulls=["product_name", "sub_category"]
    )
    check_duplicates(products, "product_id", "products")
    check_positive_values(products, "price", "products")

    # Transactions
    check_nulls(
        transactions,
        "transactions",
        allowed_nulls=["transaction_time"]
    )
    check_duplicates(transactions, "transaction_id", "transactions")

    # Transaction Items
    check_nulls(items, "transaction_items")
    check_positive_values(items, "quantity", "transaction_items")
    check_positive_values(items, "line_total", "transaction_items")

    print("All data quality checks passed successfully.")
