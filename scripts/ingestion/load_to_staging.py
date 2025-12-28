import pandas as pd
import yaml
import os
from sqlalchemy import create_engine
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

db = config["database"]

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# --------------------------------------------------
# Create database connection
# --------------------------------------------------
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# --------------------------------------------------
# Load CSVs into staging tables
# --------------------------------------------------
def load_csv_to_staging(csv_path: str, table_name: str):
    df = pd.read_csv(csv_path)

    df.to_sql(
        name=table_name,
        con=engine,
        schema="staging",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=config["pipeline"]["batch_size"]
    )

# --------------------------------------------------
# Execute ingestion
# --------------------------------------------------
if __name__ == "__main__":
    load_csv_to_staging("data/raw/customers.csv", "customers")
    load_csv_to_staging("data/raw/products.csv", "products")
    load_csv_to_staging("data/raw/transactions.csv", "transactions")
    load_csv_to_staging("data/raw/transaction_items.csv", "transaction_items")
