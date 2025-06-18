from dotenv import load_dotenv
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator


from functions.bronze import setup_postgres_infrastructure, setup_duckdb_infrastructure, verify_setup, ingestion

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 14),
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
    "data_pipeline_dbt",
    description="A DAG to execute data pipeline with dbt foor duckdb and postgreSQL",
    schedule=None,
    default_args=default_args,
    tags=['ETL', 'dbt', 'data_warehouse'],
    catchup=False,
) as dag:
    pipeline_bucket = "data-pipeline-storage"

    start_pipeline = EmptyOperator(
        task_id="start_pipeline"
    )

    create_s3_bucket = S3CreateBucketOperator(
        task_id="create_s3_bucket", bucket_name=pipeline_bucket, aws_conn_id="aws_default"
    )

    setup_duckdb_init = PythonOperator(
        task_id="setup_duckdb",
        python_callable=setup_duckdb_infrastructure,
    )

    setup_postgre_init = PythonOperator(
        task_id="setup_postgre",
        python_callable=setup_postgres_infrastructure,
    )

    verify_infrastructure_task = PythonOperator(
        task_id="verify_infrastructure",
        python_callable=verify_setup,
    )

    upload_synthetic_files = BashOperator(
        task_id="upload_synthetic_files",
        bash_command="cd /opt/airflow/dags/functions/ && python -u data_created.py"
    )

    bronze_init = EmptyOperator(
        task_id="bronze_init"
    )

    ingestion_data = PythonOperator(
        task_id="ingestion_data",
        python_callable=ingestion,
    )

    silver_init = EmptyOperator(
        task_id="silver_init"
    )

    run_dbt_silver_duck = BashOperator(
        task_id="run_dbt_silver_duck",
        bash_command="dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --target dev --select tag:silver --no-version-check"
    )

    run_dbt_silver_postgre = BashOperator(
        task_id="run_dbt_silver_postgre",
        bash_command="dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --target prod --select tag:silver --no-version-check"
    )

    gold_init = EmptyOperator(
        task_id="gold_init"
    )

    run_dbt_gold_duck = BashOperator(
        task_id="run_dbt_gold_duck",
        bash_command="dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --target dev --select tag:gold --no-version-check"
    )

    run_dbt_gold_postgre = BashOperator(
        task_id="run_dbt_gold_postgre",
        bash_command="dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --target prod --select tag:gold --no-version-check"
    )

    start_pipeline >> create_s3_bucket >> [
        setup_duckdb_init, setup_postgre_init] >> verify_infrastructure_task >> upload_synthetic_files >> bronze_init

    bronze_init >> ingestion_data >> silver_init
    silver_init >> [run_dbt_silver_duck, run_dbt_silver_postgre] >> gold_init
    gold_init >> [run_dbt_gold_duck, run_dbt_gold_postgre]
