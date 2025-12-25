import os
import time
import json
from datetime import datetime

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

OUTPUT_DIR = "data/processed/analytics"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ----------------------------
# Helpers
# ----------------------------
def execute_query(conn, sql):
    return pd.read_sql(sql, conn)

def export_to_csv(df, filename):
    df.to_csv(filename, index=False)

# ----------------------------
# Main
# ----------------------------
def main():
    start = time.time()
    summary = {
        "generation_timestamp": datetime.utcnow().isoformat(),
        "queries_executed": 0,
        "query_results": {},
        "total_execution_time_seconds": 0
    }

    with engine.connect() as conn:
        with open("sql/queries/analytical_queries.sql") as f:
            queries = f.read().split(";")

        for i, query in enumerate(queries, start=1):
            if not query.strip():
                continue

            q_start = time.time()
            df = execute_query(conn, query)
            exec_time = round((time.time() - q_start) * 1000, 2)

            filename = f"{OUTPUT_DIR}/query{i}_result.csv"
            export_to_csv(df, filename)

            summary["query_results"][f"query{i}"] = {
                "rows": len(df),
                "columns": len(df.columns),
                "execution_time_ms": exec_time
            }
            summary["queries_executed"] += 1

    summary["total_execution_time_seconds"] = round(
        time.time() - start, 2
    )

    with open(f"{OUTPUT_DIR}/analytics_summary.json", "w") as f:
        json.dump(summary, f, indent=4)

# ----------------------------
if __name__ == "__main__":
    main()
