# SQL Data Warehouse Project (Retail Analytics)

## Project Overview
This project is a data warehouse solution built to analyze retail sales data using a modern layered architecture (Bronze, Silver, Gold). It simulates a real-world analytics system used in companies to track customer behavior, product performance, and sales trends.

The project includes ETL processes, data modeling (star schema), and analytical queries to generate business insights.

## Tech Stack
- SQL Server
- T-SQL (Stored Procedures, Views, Functions)
- Data Warehouse Architecture (Bronze / Silver / Gold)
- Star Schema Data Modeling
- ETL Processes
- Data Quality Testing


## Data Architecture

The project follows a Medallion Architecture:

- 🥉 Bronze Layer: Raw data ingestion from CSV files
- 🥈 Silver Layer: Cleaned and standardized data
- 🥇 Gold Layer: Business-ready analytical tables

This layered approach ensures scalability, data quality, and separation of concerns.

## ETL Process

1. Extract raw data from source files (CSV)
2. Load into Bronze layer (no transformations)
3. Clean and transform data in Silver layer:
   - Remove duplicates
   - Standardize formats
   - Handle missing values
4. Load curated data into Gold layer for analytics

##  Business Insights

The Gold layer enables analysis of:

- Top customers by revenue
- Best-selling products
- Monthly sales trends
- Customer purchasing behavior

## Data Quality Checks

The project includes validation rules:
- Primary key uniqueness
- Null checks on key fields
- Data consistency across layers
