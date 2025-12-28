import pytest
import psycopg2
from config.config import DB_CONFIG


@pytest.fixture(scope="module")
def db_conn():
    """
    Warehouse tests require warehouse schema.
    Skip gracefully if PostgreSQL is unavailable.
    """
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            dbname=DB_CONFIG["database"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"]
        )
        yield conn
        conn.close()
    except psycopg2.OperationalError:
        pytest.skip("PostgreSQL not running — skipping warehouse tests")


def test_warehouse_tables_exist(db_conn):
    """
    Verify warehouse dimension and fact tables exist.
    """
    cursor = db_conn.cursor()
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'warehouse'
    """)
    tables = {row[0] for row in cursor.fetchall()}

    expected_tables = {
        "dim_customers",
        "dim_products",
        "dim_date",
        "dim_payment_method",
        "fact_sales"
    }

    for table in expected_tables:
        assert table in tables, f"Missing warehouse table: {table}"


def test_fact_sales_grain(db_conn):
    """
    Fact table grain should match transaction_items (line-item level).
    """
    cursor = db_conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM warehouse.fact_sales")
    fact_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM production.transaction_items")
    source_count = cursor.fetchone()[0]

    assert fact_count == source_count, (
        "fact_sales grain mismatch with transaction_items"
    )


def test_surrogate_keys_used(db_conn):
    """
    Verify surrogate keys are used in warehouse dimensions.
    """
    cursor = db_conn.cursor()

    cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'warehouse'
          AND table_name = 'dim_customers'
    """)
    columns = {row[0] for row in cursor.fetchall()}

    assert "customer_key" in columns, "Surrogate key missing in dim_customers"
    assert "customer_id" in columns, "Business key missing in dim_customers"


def test_scd_type2_columns_exist(db_conn):
    """
    Verify SCD Type 2 columns exist.
    """
    cursor = db_conn.cursor()

    cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'warehouse'
          AND table_name = 'dim_customers'
    """)
    columns = {row[0] for row in cursor.fetchall()}

    required_columns = {
        "effective_date",
        "end_date",
        "is_current"
    }

    for col in required_columns:
        assert col in columns, f"Missing SCD column: {col}"
