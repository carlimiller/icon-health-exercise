# **Icon Health ELT Pipeline: End-to-End Solution**

## **1. Overview**
This project is a **complete ELT pipeline** designed for **Icon Health**, built using **Python, dbt, and Airflow**. It processes raw MSK healthcare data, transforms it into analytics-ready tables.

## **2. Project Goals**
- **Ingest batch data** (CSV files) into a local SQLite database.
- **Process data through a Medallion Architecture**:
  - `landing_zone` (Bronze) → Raw to mapped models.
  - `standard_model` (Silver) → Transformations & enhancements.
  - `enterprise_datasets` (Gold) → Enriched datasets ready for enterprise use.
  - `feature_store` (Feature Store) → ML-ready transformations.
- **Orchestrate workflows using Apache Airflow.**
- **Run Data Quality (DQ) tests** and **handle failures.**
- **Push the entire pipeline to GitHub**.

---

## **3. Enterprise vs. Local Development**
In a real-world enterprise environment, this pipeline would be entirely cloud-based, with:
- **Automated ingestion via Fivetran (batch) and Pub/Sub or Kafka (streaming).**
- **BigQuery as the central data warehouse.**
- **Cloud Composer (Managed Airflow) for orchestration.**
- **CI/CD pipelines (GitHub Actions, dbt Cloud) for automated deployments.**
- **A managed feature store for ML readiness.**

However, since this project is being developed locally, the following adaptations are made:
- **Batch ingestion is handled by Python scripts**.
- **Data is stored in SQLite** instead of BigQuery.
- **dbt runs locally** instead of in dbt Cloud.
- **Airflow runs on a local VM** instead of Cloud Composer.

---

## **4. Repository Structure**
```plaintext
icon_health_exercise/
├── README.md
├── data
│   ├── adverse_events.csv
│   ├── employer_in_network.csv
│   ├── encounters_details.csv
│   ├── icon_health.db
│   ├── logs
│   │   └── dbt.log
│   ├── patients.csv
│   ├── providers.csv
│   └── referrals.csv
├── dbt_packages
├── elt_scripts
│   ├── data_ingestion.py
│   ├── logs
│   │   └── dbt.log
│   ├── pipeline_execution_dag.py
│   ├── raw_data_exploration.py
│   └── schema_enforcement.py
├── icon_dbt
│   ├── analyses
│   ├── dbt_packages
│   │   └── dbt_utils
│   ├── dbt_project.yml
│   ├── logs
│   │   └── dbt.log
│   ├── macros
│   ├── models
│   │   ├── enterprise_datasets
│   │   ├── feature_store
│   │   ├── landing_zone
│   │   ├── sources.yml
│   │   └── standard_model
│   ├── package-lock.yml
│   ├── packages.yml
│   ├── seeds
│   │   ├── business_entity
│   │   └── reference_table
│   ├── snapshots
│   ├── target
│   │   ├── compiled
│   └── tests
├── requirements.txt
└── venv
```

---

## **5. Data Pipeline Overview**

### ** Data Ingestion**
- **Batch Ingestion:**
  - **Current Setup:** Python script (`elt_scripts/data_ingestion.py`) reads CSV/JSON files and loads them into a SQLite database (`icon_health.db`).
  - **With Fivetran:** If used, Fivetran would automate batch ingestion, syncing structured sources directly into BigQuery.
- **Streaming Ingestion:**
  - **In production, GCP’s Pub/Sub or Kafka would handle event-driven data streams.**
- **Airflow DAGs (`elt_scripts/pipeline_execution_dag.py`)** orchestrate ingestion workflows. Airflow installation was breking my environment,
so the DAG is only a PoC and would of course live in its own folder (or repo at the enterprise level).

### ** Transformations and Data Quality with dbt**
- **Bronze (`landing_zone`)** → Raw data mapped to standard formats.
- **Silver (`standard_model`)** → Cleaning, deduplication, and key joins.
- **Gold (`enterprise_datasets`)** → Business logic, aggregations, and feature stores.
- **dbt tests (`dbt/tests/`)** ensure uniqueness, integrity, and expected values. In production, bad data would be quarantined and promoted only 
after passing subsequent DQ checks. 

### ** Optimization Strategies**
| **Component** | **Optimization Approach** | **Impact** |
|--------------|-----------------|-----------------|
| **SQLite Queries** | **Indexed primary keys** | Speeds up lookups & joins |
| **Incremental models in dbt** | **Avoids reprocessing entire datasets** | Improves efficiency |
| **Airflow DAGs** | **Optimized scheduling** | Prevents redundant runs |
| **Feature Store** | **Precomputed ML features** | Reduces query complexity for ML |

---
