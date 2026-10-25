# Olist Brazilian E-Commerce Data Engineering Pipeline

## Overview

This project is an end-to-end data engineering pipeline I built using the Olist Brazilian E-Commerce public dataset. I wanted to work with a real-world dataset and build a project that covers the main stages of a data engineering workflow, from loading raw data to creating analytical tables and visualizing the results.

I used Databricks, PySpark, and Delta Lake to process the data and followed the Medallion Architecture to organize the pipeline into Bronze, Silver, and Gold layers. I also configured a Databricks Job to connect the workflow and built a one-page dashboard directly in Databricks to explore the results.

## Tech Stack

- **Data Processing:** Python, PySpark, Spark SQL
- **Data Platform:** Databricks
- **Storage:** Delta Lake
- **Data Organization:** Unity Catalog
- **Pipeline Orchestration:** Databricks Jobs
- **Version Control:** Git and GitHub
- **Visualization:** Databricks Dashboards

## Dataset

I used the [Olist Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) from Kaggle.

The dataset contains information about orders, customers, sellers, products, payments, reviews, and delivery details. These datasets provide an opportunity to explore different aspects of an e-commerce business, including sales, fulfillment, product performance, and customer purchasing behavior.

## Pipeline Architecture

The pipeline follows the Medallion Architecture, with each layer serving a different purpose.

```text
Olist CSV Files
      |
      v
Databricks Volume
      |
      v
Bronze Layer
Raw data ingestion into Delta tables
      |
      v
Silver Layer
Data cleaning and transformation
      |
      v
Gold Layer
Business-level analytical tables
      |
      v
Databricks Dashboard
      ^
      |
Databricks Job orchestrates the workflow
```

## 1. Bronze Layer — Data Ingestion

In the Bronze layer, I loaded the original CSV files into Databricks and stored them as Delta tables.

I also added ingestion metadata, including the ingestion timestamp and source file, to help track where the data came from and when it was loaded.

This layer provides the starting point for the rest of the pipeline.

## 2. Silver Layer — Data Cleaning and Transformation

The Silver layer focuses on preparing the data for analysis.

Here, I worked on:

- Converting columns to appropriate data types.
- Cleaning and standardizing the source datasets.
- Checking important identifiers for missing values and duplicates.
- Preparing the tables for downstream joins and aggregations.
- Recording selected data-quality results in a quality audit table.

The goal was to make the data more consistent before using it to build analytical datasets.

## 3. Gold Layer — Analytics

In the Gold layer, I created tables for analyzing different parts of the e-commerce business.

The current Gold tables include:

| Table | Purpose |
|---|---|
| `gold_order_summary` | Summarizes order-level metrics |
| `gold_delivery_performance` | Supports delivery and fulfillment analysis |
| `gold_product_performance` | Analyzes product-level performance |
| `gold_category_performance` | Compares performance across product categories |
| `gold_seller_performance` | Analyzes seller-level performance |
| `gold_customer_performance` | Supports customer purchasing analysis |

One area I paid particular attention to was aggregation logic. Order items, payments, and reviews can have different numbers of records for the same order. I aggregated these datasets separately before joining them where needed to reduce the risk of double-counting metrics.

These Gold tables provide a more convenient starting point for SQL analysis and dashboard development.

## 4. Databricks Job and Dashboard

I configured a Databricks Job to bring the pipeline stages together and built a one-page dashboard directly in Databricks.

The workflow covers the Bronze ingestion, Silver transformation, and Gold analytics stages, followed by the dashboard-related task configured at the end of the job.

The dashboard provides a visual way to explore the analytical results and demonstrates how the processed data can be used beyond the transformation layer.

## What I Learned

Working on this project gave me practical experience with:

- Building multi-stage data pipelines using PySpark.
- Working with Delta tables in Databricks.
- Organizing data using the Medallion Architecture.
- Cleaning and transforming related datasets.
- Designing analytical tables for different business use cases.
- Handling joins and aggregations carefully to avoid double-counting.
- Connecting data processing workflows with Databricks Jobs.
- Building a dashboard to present analytical results.

## Future Improvements

There are a few areas I would like to improve as I continue developing the project:

- Expand data-quality checks and reconciliation between layers.
- Improve job monitoring, retry handling, and failure notifications.
- Add more metrics and visualizations to the dashboard.
- Explore incremental processing instead of rebuilding all tables.
- Add automated tests and stronger schema validation.
- Document the table schemas and business metric definitions in more detail.

## Repository Structure

```text
olist-ecommerce-data-pipeline/
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation.py
│   └── 03_gold_analytics.py
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── docs/
│   └── screenshots/
└── README.md
```

The sample data and screenshot folders are intended for small, shareable examples and dashboard screenshots. The full dataset and Delta table storage remain in Databricks rather than being committed to GitHub.

## Notes

This project uses a public historical dataset and is intended for learning and portfolio demonstration. It is not a live production system.
