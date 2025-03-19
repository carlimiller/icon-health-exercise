#In a real-world deployment, I would use Airflow or similar to orchestrate the pipeline. The provided DAG (pipeline_execution_dag.py) 
#defines tasks for file ingestionand dbt transformations. However, for this exercise, I manually executed these steps using Python 
#and dbt to simulate pipeline execution.

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'Carli',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 18),
    'retries': 1,
}

with DAG(
    'icon_health_pipeline',
    default_args=default_args,
    schedule_interval=None, #manual due to this being a local exercise
    catchup=False,
) as dag:

    ingest_files = BashOperator(
        task_id='ingest_files',
        bash_command='python etl_scripts/data_ingestion.py'
    )

    schema_enforcement = BashOperator(
        task_id='schema_enforcement',
        bash_command='python etl_scripts/schema_enforcement.py'
    )

    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command='dbt run --project-dir dbt -s tag:mapped_model+'
    )

    ingest_files >> schema_enforcement >> run_dbt_models