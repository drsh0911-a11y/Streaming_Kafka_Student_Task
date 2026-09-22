# Real-Time & Batch E-Commerce Data Platform

## 🚀 Project Overview
This project is an end-to-end data engineering platform designed to ingest, process, and transform high-volume e-commerce data. It solves the business problem of fragmented data silos by providing clean, unified, and real-time analytics-ready models for downstream business intelligence and machine learning applications.

### Core Tech Stack
* **Orchestration:** Apache Airflow
* **Processing:** PySpark
* **Transformation:** dbt (Data Build Tool)
* **Data Warehouse / Storage:** Snowflake
* **Streaming / Messaging:** Apache Kafka *(if applicable)*

---

## 📊 Architecture Flowchart
The following diagram illustrates how data flows from the source systems through the processing layers down to the data warehouse:

```mermaid
graph TD
    A[Data Sources / Kafka Stream] -->|Ingestion| B[PySpark Processing Layer]
    B -->|Raw / Staging Data| C[(Snowflake: Bronze Layer)]
    C -->|Transformation & Cleaning| D[(Snowflake: Silver Layer)]
    D -->|Aggregations & Business Logic| E[(Snowflake: Gold Layer)]
    F[Apache Airflow] -->|Orchestrates DAGs| B
    F -->|Triggers Runs| D