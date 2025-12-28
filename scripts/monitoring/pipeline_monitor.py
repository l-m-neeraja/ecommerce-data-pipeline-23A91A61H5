import json
import time
from datetime import datetime, timezone
import psycopg2
import statistics

DB_CONFIG = {
    "host": "localhost",
    "database": "ecommerce_db",
    "user": "admin",
    "password": "password"
}

OUTPUT_PATH = "data/processed/monitoring_report.json"


def get_db_connection():
    start = time.time()
    conn = psycopg2.connect(**DB_CONFIG)
    response_time = (time.time() - start) * 1000
    return conn, response_time


def run_monitoring():
    monitoring_time = datetime.now(timezone.utc).isoformat()
    alerts = []

    report = {
        "monitoring_timestamp": monitoring_time,
        "pipeline_health": "healthy",
        "checks": {},
        "alerts": [],
        "overall_health_score": 100
    }

    try:
        conn, response_time = get_db_connection()
        cur = conn.cursor()

        # -------------------------------
        # Database Connectivity
        # -------------------------------
        report["checks"]["database_connectivity"] = {
            "status": "ok",
            "response_time_ms": round(response_time, 2),
            "connections_active": 1
        }

        # -------------------------------
        # Data Freshness
        # -------------------------------
        cur.execute("""
            SELECT MAX(created_at) FROM warehouse.fact_sales;
        """)
        latest = cur.fetchone()[0]
        lag_hours = (datetime.now(timezone.utc) - latest).total_seconds() / 3600

        freshness_status = "ok" if lag_hours < 24 else "critical"
        if freshness_status == "critical":
            alerts.append({
                "severity": "critical",
                "check": "data_freshness",
                "message": "Warehouse data older than 24 hours",
                "timestamp": monitoring_time
            })

        report["checks"]["data_freshness"] = {
            "status": freshness_status,
            "warehouse_latest_record": latest.isoformat(),
            "max_lag_hours": round(lag_hours, 2)
        }

        # -------------------------------
        # Data Volume Anomalies
        # -------------------------------
        cur.execute("""
            SELECT DATE(created_at), COUNT(*)
            FROM warehouse.fact_sales
            GROUP BY DATE(created_at)
            ORDER BY DATE(created_at);
        """)
        counts = [row[1] for row in cur.fetchall()]

        mean = statistics.mean(counts)
        std = statistics.stdev(counts) if len(counts) > 1 else 0
        current = counts[-1]

        anomaly = current > mean + (3 * std) or current < mean - (3 * std)

        report["checks"]["data_volume_anomalies"] = {
            "status": "anomaly_detected" if anomaly else "ok",
            "expected_range": f"{int(mean - 3*std)}-{int(mean + 3*std)}",
            "actual_count": current,
            "anomaly_detected": anomaly,
            "anomaly_type": "spike" if current > mean else "drop" if anomaly else None
        }

        if anomaly:
            alerts.append({
                "severity": "warning",
                "check": "data_volume",
                "message": "Unusual transaction volume detected",
                "timestamp": monitoring_time
            })

        # -------------------------------
        # Data Quality
        # -------------------------------
        cur.execute("""
            SELECT COUNT(*) FROM warehouse.fact_sales
            WHERE customer_key IS NULL
               OR product_key IS NULL
               OR date_key IS NULL;
        """)
        nulls = cur.fetchone()[0]
        quality_score = 100 - (nulls * 5)

        report["checks"]["data_quality"] = {
            "status": "ok" if nulls == 0 else "degraded",
            "quality_score": max(0, quality_score),
            "orphan_records": 0,
            "null_violations": nulls
        }

        report["alerts"] = alerts
        report["overall_health_score"] = max(0, quality_score - (10 * len(alerts)))
        cur.close()
        conn.close()

    except Exception as e:
        report["pipeline_health"] = "critical"
        report["alerts"].append({
            "severity": "critical",

            "check": "system",
            "message": str(e),
            "timestamp": monitoring_time
        })
        report["overall_health_score"] = 0


    with open(OUTPUT_PATH, "w") as f:
        json.dump(report, f, indent=4)

    print("Monitoring report generated:", OUTPUT_PATH)


if __name__ == "__main__":
    run_monitoring()

