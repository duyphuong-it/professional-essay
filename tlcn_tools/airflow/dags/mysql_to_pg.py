from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import numpy as np

# ------------------------------
# CONFIG
# ------------------------------
TABLES = ["locations", "payments", "vehicles", "ride_bookings"]
SCHEMA = "bronze"

# ------------------------------
# FUNCTIONS
# ------------------------------
def create_schema():
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    pg_hook.run(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")

def transfer_table(table_name: str):
    mysql_hook = MySqlHook(mysql_conn_id="mysql_default")
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")

    df = mysql_hook.get_pandas_df(f"SELECT * FROM {table_name}")

    # Làm sạch dữ liệu nếu có các cột cần xử lý
    if table_name == "ride_bookings":
        df["booking_id"] = df["booking_id"].str.replace('"', '', regex=False)
        df["customer_id"] = df["customer_id"].str.replace('"', '', regex=False)
        # Chuyển cột time về dạng chuỗi nếu là timedelta64
        if "time" in df.columns and np.issubdtype(df["time"].dtype, np.timedelta64):
            df["time"] = df["time"].apply(lambda x: str(x).split()[-1] if pd.notnull(x) else None)

    # Đẩy dữ liệu sang PostgreSQL
    df.to_sql(
        name=table_name,
        con=pg_hook.get_sqlalchemy_engine(),
        schema=SCHEMA,
        if_exists="replace",
        index=False
    )

# ------------------------------
# DAG DEFINITION
# ------------------------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
}

with DAG(
    dag_id="mysql_to_postgres_bronze",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="ETL MySQL -> Postgres (bronze layer)"
) as dag:

    create_schema_task = PythonOperator(
        task_id="create_bronze_schema",
        python_callable=create_schema
    )

    for table in TABLES:
        transfer_task = PythonOperator(
            task_id=f"transfer_{table}",
            python_callable=transfer_table,
            op_args=[table],
        )

        create_schema_task >> transfer_task