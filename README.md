# E-Commerce ETL Pipeline with PySpark
## Overview

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline using Apache Spark (PySpark) to process e-commerce transactional data. The pipeline performs data ingestion, data quality checks, transformations, analytics, return analysis, and writes curated datasets to Parquet format for downstream reporting and analytics.

The solution demonstrates core data engineering concepts including:

Schema enforcement

Data cleansing and validation

Data enrichment

Data quality monitoring

Complex joins

Window functions

Aggregations

Revenue analytics

Return analysis

Parquet partitioning

## ## Architecture

```text
CSV Files
    │
    ▼
Data Ingestion
    │
    ▼
Schema Enforcement
    │
    ▼
Data Cleaning & Validation
    │
    ▼
Data Transformations
    │
    ▼
Business Analytics
    │
    ├── Revenue Analysis
    ├── Customer Analytics
    ├── Return Analysis
    └── Data Quality Checks
    │
    ▼
Parquet Outputs
```

## Project Structure
```text
project/
│ 
├── csv/ 
│   ├── customers.csv
│   ├── orders.csv
│   ├── order_items.csv
│   └── returns.csv
│
├── parquet/ 
│
├── ETL.py 
│
└── README.m 
```

## Technologies Used

Apache Spark

PySpark

Python

Parquet

Window Functions

DataFrame API


## Running the Project locally

### Prerequisites

Python 3.10+

Apache Spark

PySpark


1. Clone the repository
```
https://github.com/GeraldM-007/Crypto-Market-Data-Engineering-Project.git

cd Crypto-Market-Data-Engineering-Project
```
2. Install dependencies:
```
pip install -r requirements.txt
```
3. Execute:
   ```
   python ETL.py
   ```
