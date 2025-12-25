import os
import json
import time
from datetime import datetime

import pandas as pd
import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# ----------------------------
# Load environment & config
# ----------------------------
load_dotenv()

with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

RAW_DATA_DIR = "data/raw"
STAGING_SCHEMA = "staging"

# ----------------------------
# Database connection
# ----------------------------
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    pool_size=5,
    max_overflow=10
)

# ----------------------------
# Ingestion function
# ----------------------------
def ingest_table(conn, table_name, csv_file):
    df = pd.read_csv(csv_file)

    conn.execute(text(f"TRUNCATE TABLE {STAGING_SCHEMA}.{table_name}"))

    df.to_sql(
        table_name,
        con=conn,
        schema=STAGING_SCHEMA,
        if_exists="append",
        index=False,
        method="multi"
    )

    return len(df)

# ----------------------------
# Validation function
# ----------------------------
def validate_staging_load(conn, table_name, expected_count):
    result = conn.execute(
        text(f"SELECT COUNT(*) FROM {STAGING_SCHEMA}.{table_name}")
    )
    actual_count = result.scalar()
    return actual_count == expected_count, actual_count

# ----------------------------
# Main execution
# ----------------------------
def main():
    start_time = time.time()
    summary = {
        "ingestion_timestamp": datetime.utcnow().isoformat(),
        "tables_loaded": {},
        "total_execution_time_seconds": 0
    }

    try:
        with engine.begin() as conn:
            tables = {
                "customers": "customers.csv",
                "products": "products.csv",
                "transactions": "transactions.csv",
                "transaction_items": "transaction_items.csv"
            }

            for table, file in tables.items():
                csv_path = os.path.join(RAW_DATA_DIR, file)

                if not os.path.exists(csv_path):
                    raise FileNotFoundError(f"Missing file: {file}")

                rows_loaded = ingest_table(conn, table, csv_path)

                valid, actual = validate_staging_load(conn, table, rows_loaded)

                summary["tables_loaded"][f"{STAGING_SCHEMA}.{table}"] = {
                    "rows_loaded": rows_loaded,
                    "status": "success" if valid else "failed",
                    "error_message": None if valid else "Row count mismatch"
                }

    except (SQLAlchemyError, FileNotFoundError, Exception) as e:
        summary["error"] = str(e)
        raise

    finally:
        summary["total_execution_time_seconds"] = round(
            time.time() - start_time, 2
        )

        os.makedirs("data/staging", exist_ok=True)
        with open("data/staging/ingestion_summary.json", "w") as f:
            json.dump(summary, f, indent=4)

# ----------------------------
if __name__ == "__main__":
    main()
