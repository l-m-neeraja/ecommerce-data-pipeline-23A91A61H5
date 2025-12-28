# E-Commerce Data Pipeline Project

**Student Name:** Padala Leela Mallika Neeraja 
**Roll Number:** 23A91A61H5
**Submission Date:** 29-12-2025  

## Project Overview
This project implements an end-to-end e-commerce data pipeline that generates synthetic data, processes it through multiple layers (staging, production, warehouse), performs analytics, and visualizes insights using a BI dashboard.

The system follows modern data engineering best practices including schema separation, dimensional modeling, automation, testing, and documentation.

---

## Project Architecture

### Data Flow

Raw Data (CSV)
↓
Staging Schema (PostgreSQL)
↓
Production Schema (Cleaned & Validated)
↓
Warehouse Schema (Star Schema)
↓
Analytics Queries
↓
BI Dashboard (Power BI / Tableau)

---

## Technology Stack

| Layer | Technology |
|------|-----------|
| Data Generation | Python, Faker |
| Database | PostgreSQL |
| ETL & Transformation | Python (Pandas, SQLAlchemy) |
| Analytics | SQL |
| Orchestration | Python Scheduler |
| BI Tool | Power BI Desktop |
| Testing | Pytest |
| Version Control | Git & GitHub |


## Project Structure
```
ecommerce-data-pipeline/
│
├── config/
│   └── config.yaml
│
├── scripts/
│   ├── data_generation/
│   ├── ingestion/
│   ├── quality_checks/
│   ├── transformation/
│   ├── monitoring/
│   ├── pipeline_orchestrator.py
│   ├── scheduler.py
│   └── cleanup_old_data.py
│
├── sql/
│   ├── ddl/
│   └── queries/
│
├── data/
│   ├── raw/
│   ├── processed/
│
├── dashboards/
│   ├── powerbi/
│   └── screenshots/
│
├── tests/
│
├── docs/
│
└── README.md

---

## Database Schemas

### Staging Schema
- staging.customers
- staging.products
- staging.transactions
- staging.transaction_items

Purpose: Raw ingestion from CSV without heavy validation.

---

### Production Schema
- production.customers
- production.products
- production.transactions
- production.transaction_items

Purpose: Cleaned, validated, normalized transactional data.

---

### Warehouse Schema
- warehouse.dim_customers
- warehouse.dim_products
- warehouse.dim_date
- warehouse.dim_payment_method
- warehouse.fact_sales
- Aggregate tables for performance optimization

Purpose: Optimized for analytical queries and BI tools using a star schema.

---

## Running the Pipeline

### Full Pipeline Execution

python scripts/pipeline_orchestrator.py

### Individual Steps

python scripts/data_generation/generate_data.py  
python scripts/ingestion/load_to_staging.py  
python scripts/transformation/staging_to_production.py  
python scripts/transformation/load_warehouse.py  
python scripts/transformation/generate_analytics.py  

---

## Running Tests

pytest tests/ -v

Tests include:
- Data generation validation
- Ingestion checks
- Transformation rules
- Data quality checks
- Warehouse integrity checks

---

## Dashboard Access

### Power BI
- File: dashboards/powerbi/ecommerce_analytics.pbix
- Screenshots: dashboards/screenshots/

---

## Key Insights from Analytics
- Top product categories contribute majority of revenue
- Revenue shows consistent monthly growth
- High-value customers contribute disproportionate revenue
- Weekend sales outperform weekday sales
- Digital payment methods dominate transactions

---

## Challenges & Solutions

Challenge: PostgreSQL availability during testing  
Solution: Implemented graceful test skipping with Pytest fixtures  

Challenge: Maintaining idempotency  
Solution: Truncate-and-reload strategy with controlled transformations  

Challenge: Complex evaluation requirements  
Solution: Followed exact folder structure, naming conventions, and schemas  

---

## Future Enhancements
- Real-time ingestion using Kafka
- Cloud deployment (AWS / GCP)
- ML-based demand forecasting
- Real-time alerting and monitoring dashboards

---

## Contact
Name: Padala Leela Mallika Neeraja  
Program: B.Tech  
Specialization: AI & ML

