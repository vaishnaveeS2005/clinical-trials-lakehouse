# Clinical Trial Intelligence Lakehouse Architecture

## Overview

This project implements a Medallion Architecture on Databricks for clinical trial operational analytics.

The platform ingests raw clinical trial datasets into Bronze Delta tables using PySpark and Delta Lake.

The architecture is designed for scalable healthcare analytics, governance, data quality, and KPI reporting.

---

## Architecture Flow

Raw CSV Files
↓
Databricks Volumes Landing Zone
↓
Bronze Delta Tables
↓
Silver Cleaned and Conformed Tables
↓
Gold Business KPI Marts
↓
Dashboards and Analytics

---

## Technologies

- Databricks
- PySpark
- Delta Lake
- Unity Catalog
- Databricks Volumes
- Spark SQL

---

## Week 1 Components Implemented

- Unity Catalog setup
- Volume-based raw landing zone
- Bronze ingestion pipeline
- Metadata columns
- Delta Lake tables
- Table verification

---

## Future Enhancements

- Silver transformations
- Data quality framework
- Quarantine tables
- SCD Type 2
- Gold KPI marts
- Databricks Workflows
- Lakeflow Declarative Pipelines
