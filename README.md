# E-Commerce Data Pipeline Project

Student Name: Padala Leela Mallika Neeraja  
Roll Number: 23A91A61H5
Submission Date: 27-12-2025

## Project Overview
This project implements an end-to-end e-commerce data engineering pipeline that covers data generation, ingestion, transformation, data quality checks, and analytical reporting using SQL and BI tools.

## Prerequisites
- Python 3.8 or above
- PostgreSQL 12 or above
- Docker & Docker Compose
- Git
- Tableau Public OR Power BI Desktop (Free version)

## Installation Steps
1. Clone the repository:
   git clone https://github.com/l-m-neeraja/ecommerce-data-pipeline-23A91A61H5

2. Navigate to the project directory:
   cd ecommerce-data-pipeline-23A91A61H5

3. Install Python dependencies:
   pip install -r requirements.txt

4. Setup PostgreSQL database (or use Docker):
   Database Name: ecommerce_db

5. Run the setup script:
   bash setup.sh

6. Install BI Tool:
   - Tableau Public OR
   - Power BI Desktop

## Database Configuration
- Database Name: ecommerce_db
- Staging Schema: staging
- Production Schema: production
- Warehouse Schema: warehouse

## Configuration Management
- Environment variables are managed using a `.env` file
- Application configuration is stored in `config/config.yaml`
- Sensitive credentials are not hardcoded in the source code
