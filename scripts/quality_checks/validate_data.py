import os
import json
from datetime import datetime

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

REPORT_DIR = "data/quality"
os.makedirs(REPORT_DIR, exist_ok=True)

# ----------------------------
# Helper function
# ----------------------------
def run_query(query):
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return result.fetchall()

# ----------------------------
# Main Quality Checks
# ----------------------------
def main():
    checks = {}
    total_violations = 0

    # Completeness
    nulls = run_query("""
        SELECT COUNT(*) FROM production.customers WHERE email IS NULL
    """)[0][0]

    checks["null_checks"] = {
        "status": "passed" if nulls == 0 else "failed",
        "null_violations": nulls,
        "details": {"customers.email": nulls}
    }
    total_violations += nulls

    # Duplicate emails
    duplicates = run_query("""
        SELECT COUNT(*) FROM (
            SELECT email FROM production.customers
            GROUP BY email HAVING COUNT(*) > 1
        ) x
    """)[0][0]

    checks["duplicate_checks"] = {
        "status": "passed" if duplicates == 0 else "failed",
        "duplicates_found": duplicates
    }
    total_violations += duplicates

    # Referential integrity
    orphans = run_query("""
        SELECT COUNT(*) FROM production.transactions t
        LEFT JOIN production.customers c
        ON t.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """)[0][0]

    checks["referential_integrity"] = {
        "status": "passed" if orphans == 0 else "failed",
        "orphan_records": orphans
    }
    total_violations += orphans

    # Consistency
    mismatches = run_query("""
        SELECT COUNT(*) FROM production.transaction_items
        WHERE ABS(
            line_total - (quantity * unit_price * (1 - discount_percentage/100))
        ) > 0.01
    """)[0][0]

    checks["data_consistency"] = {
        "status": "passed" if mismatches == 0 else "failed",
        "mismatches": mismatches
    }
    total_violations += mismatches

    # Scoring
    score = max(0, 100 - total_violations)
    grade = (
        "A" if score >= 90 else
        "B" if score >= 80 else
        "C" if score >= 70 else
        "D" if score >= 60 else
        "F"
    )

    report = {
        "check_timestamp": datetime.utcnow().isoformat(),
        "checks_performed": checks,
        "overall_quality_score": score,
        "quality_grade": grade
    }

    with open(f"{REPORT_DIR}/quality_report.json", "w") as f:
        json.dump(report, f, indent=4)

# ----------------------------
if __name__ == "__main__":
    main()
