# Clinical Trial Intelligence Lakehouse on Databricks

## Project Overview

This project implements an end-to-end Clinical Trial Intelligence Lakehouse on Databricks using PySpark, Delta Lake, Unity Catalog, and Medallion Architecture.

The solution ingests synthetic clinical trial operational datasets into Bronze Delta tables and prepares the foundation for Silver transformations, Gold business marts, analytics dashboards, and workflow orchestration.

---

## Business Problem

Clinical trial sponsors and CROs require trusted data platforms to monitor:

- Patient enrollment
- Site performance
- Adverse events
- Protocol deviations
- Drug inventory and supply risk
- Trial risk indicators

This project simulates a production-grade healthcare lakehouse platform for clinical trial operations analytics.

---

## Technologies Used

- Databricks
- PySpark
- Spark SQL
- Delta Lake
- Unity Catalog
- Databricks Volumes
- Medallion Architecture
- Databricks Workflows
- Lakeflow Declarative Pipelines

---

## Architecture

Raw Files → Bronze Delta Tables → Silver Cleaned Tables → Gold KPI Marts → Dashboards

---

## Week 1 Implementation

Completed components:

- Unity Catalog setup
- Volume-based raw landing zone
- Bronze layer ingestion
- Delta table creation
- Metadata tracking
- Schema inference and validation

---

## Bronze Tables

- bronze_trials
- bronze_sites
- bronze_patients
- bronze_visits
- bronze_adverse_events
- bronze_protocol_deviations
- bronze_drug_inventory
- bronze_drug_shipments
- bronze_lab_results
- bronze_reference_codes

---

## Metadata Columns

Each Bronze table includes:

- ingestion_timestamp
- source_file_name
- batch_id
- record_hash

---

## Project Structure

```text
clinical-trials-lakehouse/
│
├── notebooks/
├── src/
├── tests/
├── sql/
├── docs/
├── resources/
├── screenshots/
└── README.md
```

---

## Upcoming Phases

- Silver layer transformations
- Data quality validations
- Quarantine tables
- SCD Type 2 implementation
- Gold KPI marts
- Risk scoring models
- Databricks Workflows
- Lakeflow pipelines
- Asset Bundle deployment

---

## Author

Intern Data Engineering Project – Clinical Trial Intelligence Lakehouse
