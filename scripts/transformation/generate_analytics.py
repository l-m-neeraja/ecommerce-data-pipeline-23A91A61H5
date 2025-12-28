import os
import time
import json
import yaml
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ============================================================
# Phase 4.1 : Analytics Generation Script
# Purpose:
#   Execute analytical SQL queries and export results to CSV
# ============================================================

# ------------------------------------------------------------
# Load environment variables
# ------------------------------------------------------------
load_dotenv()

# ------------------------------------------------------------
# Load configuration
# ------------------------------------------------------------
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

# ------------------------------------------------------------
# Database connection (PostgreSQL-compatible)
# ------------------------------------------------------------
engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

# ------------------------------------------------------------
# Output directory
# ------------------------------------------------------------
OUTPUT_DIR = "data/processed/analytics"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ------------------------------------------------------------
# Required Functions
# ------------------------------------------------------------
def execute_query(connection, query_name: str, sql: str) -> pd.DataFrame:
    """
    Execute a SQL query and return result as DataFrame
    """
    start_time = time.time()
    df = pd.read_sql(text(sql), connection)
    execution_time = (time.time() - start_time) * 1000  # ms
    return df, execution_time


def export_to_csv(dataframe: pd.DataFrame, filename: str):
    """
    Export DataFrame to CSV
    """
    filepath = os.path.join(OUTPUT_DIR, filename)
    dataframe.to_csv(filepath, index=False)


def generate_summary(results: dict) -> dict:
    """
    Generate analytics execution summary
    """
    total_time = sum(v["execution_time_ms"] for v in results.values()) / 1000

    return {
        "generation_timestamp": datetime.utcnow().isoformat(),
        "queries_executed": len(results),
        "query_results": results,
        "total_execution_time_seconds": round(total_time, 2)
    }

# ------------------------------------------------------------
# Analytical Queries (Warehouse Schema)
# ------------------------------------------------------------
ANALYTICAL_QUERIES = {
    "query1_top_products": {
        "filename": "query1_top_products.csv",
        "sql": """
            SELECT
                dp.product_name,
                dp.category,
                SUM(fs.total_amount) AS total_revenue,
                SUM(fs.quantity) AS units_sold,
                AVG(fs.unit_price) AS avg_price
            FROM warehouse.fact_sales fs
            JOIN warehouse.dim_products dp
                ON fs.product_key = dp.product_key
            WHERE dp.is_current = TRUE
            GROUP BY dp.product_name, dp.category
            ORDER BY total_revenue DESC
            LIMIT 10;
        """
    },

    "query2_monthly_trend": {
        "filename": "query2_monthly_trend.csv",
        "sql": """
            SELECT
                CONCAT(dd.year, '-', LPAD(dd.month::TEXT, 2, '0')) AS year_month,
                SUM(fs.total_amount) AS total_revenue,
                COUNT(DISTINCT fs.transaction_id) AS total_transactions,
                AVG(fs.total_amount) AS average_order_value,
                COUNT(DISTINCT fs.customer_key) AS unique_customers
            FROM warehouse.fact_sales fs
            JOIN warehouse.dim_date dd
                ON fs.date_key = dd.date_key
            GROUP BY dd.year, dd.month
            ORDER BY dd.year, dd.month;
        """
    },

    "query3_customer_segmentation": {
        "filename": "query3_customer_segmentation.csv",
        "sql": """
            WITH customer_totals AS (
                SELECT
                    customer_key,
                    SUM(total_amount) AS total_spent
                FROM warehouse.fact_sales
                GROUP BY customer_key
            )
            SELECT
                CASE
                    WHEN total_spent < 1000 THEN '$0-$1,000'
                    WHEN total_spent < 5000 THEN '$1,000-$5,000'
                    WHEN total_spent < 10000 THEN '$5,000-$10,000'
                    ELSE '$10,000+'
                END AS spending_segment,
                COUNT(*) AS customer_count,
                SUM(total_spent) AS total_revenue,
                AVG(total_spent) AS avg_transaction_value
            FROM customer_totals
            GROUP BY spending_segment;
        """
    },

    "query4_category_performance": {
        "filename": "query4_category_performance.csv",
        "sql": """
            SELECT
                dp.category,
                SUM(fs.total_amount) AS total_revenue,
                SUM(fs.total_amount - (fs.quantity * dp.cost)) AS total_profit,
                ROUND(
                    (SUM(fs.total_amount - (fs.quantity * dp.cost))
                    / NULLIF(SUM(fs.total_amount), 0)) * 100, 2
                ) AS profit_margin_pct,
                SUM(fs.quantity) AS units_sold
            FROM warehouse.fact_sales fs
            JOIN warehouse.dim_products dp
                ON fs.product_key = dp.product_key
            WHERE dp.is_current = TRUE
            GROUP BY dp.category;
        """
    },

    "query5_payment_distribution": {
        "filename": "query5_payment_distribution.csv",
        "sql": """
            SELECT
                payment_method,
                COUNT(*) AS transaction_count,
                SUM(total_amount) AS total_revenue,
                ROUND(
                    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2
                ) AS pct_of_transactions,
                ROUND(
                    SUM(total_amount) * 100.0 / SUM(SUM(total_amount)) OVER (), 2
                ) AS pct_of_revenue
            FROM production.transactions
            GROUP BY payment_method;
        """
    },

    "query6_geographic_analysis": {
        "filename": "query6_geographic_analysis.csv",
        "sql": """
            SELECT
                dc.state,
                SUM(fs.total_amount) AS total_revenue,
                COUNT(DISTINCT fs.customer_key) AS total_customers,
                AVG(fs.total_amount) AS avg_revenue_per_customer
            FROM warehouse.fact_sales fs
            JOIN warehouse.dim_customers dc
                ON fs.customer_key = dc.customer_key
            WHERE dc.is_current = TRUE
            GROUP BY dc.state;
        """
    },

    "query7_customer_lifetime_value": {
        "filename": "query7_customer_lifetime_value.csv",
        "sql": """
            SELECT
                dc.customer_id,
                CONCAT(dc.first_name, ' ', dc.last_name) AS full_name,
                SUM(fs.total_amount) AS total_spent,
                COUNT(DISTINCT fs.transaction_id) AS transaction_count,
                CURRENT_DATE - dc.registration_date AS days_since_registration,
                AVG(fs.total_amount) AS avg_order_value
            FROM warehouse.fact_sales fs
            JOIN warehouse.dim_customers dc
                ON fs.customer_key = dc.customer_key
            WHERE dc.is_current = TRUE
            GROUP BY dc.customer_id, dc.first_name, dc.last_name, dc.registration_date;
        """
    },

    "query8_product_profitability": {
        "filename": "query8_product_profitability.csv",
        "sql": """
            SELECT
                dp.product_name,
                dp.category,
                SUM(fs.total_amount - (fs.quantity * dp.cost)) AS total_profit,
                ROUND(
                    (SUM(fs.total_amount - (fs.quantity * dp.cost))
                    / NULLIF(SUM(fs.total_amount), 0)) * 100, 2
                ) AS profit_margin,
                SUM(fs.total_amount) AS revenue,
                SUM(fs.quantity) AS units_sold
            FROM warehouse.fact_sales fs
            JOIN warehouse.dim_products dp
                ON fs.product_key = dp.product_key
            WHERE dp.is_current = TRUE
            GROUP BY dp.product_name, dp.category;
        """
    },

    "query9_day_of_week_pattern": {
        "filename": "query9_day_of_week_pattern.csv",
        "sql": """
            SELECT
                dd.day_name,
                AVG(fs.total_amount) AS avg_daily_revenue,
                AVG(COUNT(DISTINCT fs.transaction_id))
                    OVER (PARTITION BY dd.day_name) AS avg_daily_transactions,
                SUM(fs.total_amount) AS total_revenue
            FROM warehouse.fact_sales fs
            JOIN warehouse.dim_date dd
                ON fs.date_key = dd.date_key
            GROUP BY dd.day_name, dd.day_of_week
            ORDER BY dd.day_of_week;
        """
    },

    "query10_discount_impact": {
        "filename": "query10_discount_impact.csv",
        "sql": """
            SELECT
                CASE
                    WHEN discount_amount = 0 THEN '0%'
                    WHEN discount_amount <= 10 THEN '1-10%'
                    WHEN discount_amount <= 25 THEN '11-25%'
                    WHEN discount_amount <= 50 THEN '26-50%'
                    ELSE '50%+'
                END AS discount_range,
                AVG(discount_amount) AS avg_discount_pct,
                SUM(quantity) AS total_quantity_sold,
                SUM(total_amount) AS total_revenue,
                AVG(total_amount) AS avg_line_total
            FROM warehouse.fact_sales
            GROUP BY discount_range;
        """
    }
}

# ------------------------------------------------------------
# Main Execution (Optional)
# ------------------------------------------------------------
if __name__ == "__main__":
    results_summary = {}

    with engine.connect() as conn:
        for name, query_info in ANALYTICAL_QUERIES.items():
            df, exec_time = execute_query(conn, name, query_info["sql"])
            export_to_csv(df, query_info["filename"])

            results_summary[name] = {
                "rows": len(df),
                "columns": len(df.columns),
                "execution_time_ms": round(exec_time, 2)
            }

    summary = generate_summary(results_summary)

    with open(os.path.join(OUTPUT_DIR, "analytics_summary.json"), "w") as f:
        json.dump(summary, f, indent=4)

    print("Analytics generation completed successfully.")
