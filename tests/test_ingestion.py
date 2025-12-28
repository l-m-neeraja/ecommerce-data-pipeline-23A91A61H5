import pytest
import psycopg2
from config.config import DB_CONFIG


@pytest.fixture(scope="module")
def db_conn():
    """
    Database connection fixture.
    If PostgreSQL is not running, ingestion tests are skipped gracefully.
    This is intentional and documented behavior.
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
        pytest.skip("PostgreSQL not running — skipping ingestion DB tests")


def test_database_connection(db_conn):
    """
    Verify database connection is established.
    """
    assert db_conn is not None


def test_staging_tables_exist(db_conn):
    """
    Verify all required staging tables exist.
    """
    cursor = db_conn.cursor()
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'staging'
    """)
    tables = {row[0] for row in cursor.fetchall()}

    expected_tables = {
        "customers",
        "products",
        "transactions",
        "transaction_items"
    }

    for table in expected_tables:
        assert table in tables, f"Missing staging table: {table}"


def test_staging_tables_have_data(db_conn):
    """
    Verify staging tables contain data.
    """
    cursor = db_conn.cursor()

    for table in ["customers", "products", "transactions", "transaction_items"]:
        cursor.execute(f"SELECT COUNT(*) FROM staging.{table}")
        count = cursor.fetchone()[0]
        assert count > 0, f"Table staging.{table} is empty"


def test_loaded_at_column_exists(db_conn):
    """
    Verify loaded_at timestamp column exists in staging tables.
    """
    cursor = db_conn.cursor()

    for table in ["customers", "products", "transactions", "transaction_items"]:
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'staging'
              AND table_name = %s
              AND column_name = 'loaded_at'
        """, (table,))
        result = cursor.fetchone()
        assert result is not None, f"'loaded_at' missing in staging.{table}"
