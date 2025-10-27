from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

def drop_schemas():
    """Xóa hết schema trong PostgreSQL để thực hiện lại pipeline"""
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    pg_hook.run(f"DROP SCHEMA IF EXISTS bronze CASCADE;")
    pg_hook.run(f"DROP SCHEMA IF EXISTS silver CASCADE;")
    pg_hook.run(f"DROP SCHEMA IF EXISTS gold CASCADE;")

# ------------------------------
# DAG DEFINITION
# ------------------------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
}

with DAG(
    dag_id="refresh_transform_pipeline",
    default_args=default_args,
    schedule_interval=None,  # chỉ chạy khi trigger thủ công
    catchup=False,
    description="Drop all schemas in Postgres to refresh the transformation pipeline"
) as dag:

    drop_schemas_task = PythonOperator(
        task_id="drop_schemas",
        python_callable=drop_schemas
    )

    dbt_clean = BashOperator(
        task_id=f"dbt_clean",
        bash_command=(
            "cd /opt/airflow/dbt_project && dbt clean"
        )
    )

    drop_schemas_task >> dbt_clean