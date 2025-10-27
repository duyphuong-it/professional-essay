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
    """Tạo schema bronze trong PostgreSQL nếu chưa có"""
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    pg_hook.run(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")


def transfer_table(table_name: str):
    """Incremental ingestion từ MySQL -> PostgreSQL"""
    mysql_hook = MySqlHook(mysql_conn_id="mysql_default")
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    pg_engine = pg_hook.get_sqlalchemy_engine()

    # ------------------------------
    # 1️⃣ Lấy timestamp mới nhất đã load trong PostgreSQL
    # ------------------------------
    last_ts = None
    try:
        query_last_ts = f"""
            SELECT MAX(TO_TIMESTAMP(CONCAT(date, ' ', time), 'YYYY-MM-DD HH24:MI:SS')) AS last_ts
            FROM {SCHEMA}.{table_name};
        """
        last_ts_df = pd.read_sql(query_last_ts, pg_engine)
        last_ts = last_ts_df["last_ts"][0]
    except Exception:
        # Bảng chưa tồn tại
        last_ts = None

    if pd.isna(last_ts) or last_ts is None:
        last_ts = datetime(1900, 1, 1)
        print(f"⚙️ No existing data found for {table_name}, performing initial load.")
    else:
        print(f"⚙️ Last ingested timestamp for {table_name}: {last_ts}")

    # ------------------------------
    # 2️⃣ Lấy dữ liệu mới hơn từ MySQL
    # ------------------------------
    # Nếu bảng có cột Date & Time thì mới làm incremental
    if table_name == "ride_bookings":
        query = f"""
            SELECT *,
                   STR_TO_DATE(CONCAT(Date, ' ', Time), '%Y-%m-%d %H:%i:%s') AS datetime_ingested
            FROM {table_name}
            WHERE STR_TO_DATE(CONCAT(Date, ' ', Time), '%Y-%m-%d %H:%i:%s') > '{last_ts}';
        """
    else:
        # Các bảng khác tạm thời full load (vì chưa có cột thời gian)
        query = f"SELECT * FROM {table_name};"

    df = mysql_hook.get_pandas_df(query)

    if df.empty:
        print(f"✅ No new data for table '{table_name}'. Skipping load.")
        return

    # ------------------------------
    # 3️⃣ Làm sạch dữ liệu (chỉ cho bảng ride_bookings)
    # ------------------------------
    if table_name == "ride_bookings":
        df["booking_id"] = df["booking_id"].astype(str).str.replace('"', '', regex=False)
        df["customer_id"] = df["customer_id"].astype(str).str.replace('"', '', regex=False)

        if "time" in df.columns and np.issubdtype(df["time"].dtype, np.timedelta64):
            df["time"] = df["time"].apply(lambda x: str(x).split()[-1] if pd.notnull(x) else None)

        # Thêm cột load_timestamp để tracking batch
        df["load_timestamp"] = datetime.now()

    # ------------------------------
    # 4️⃣ Append dữ liệu mới vào PostgreSQL
    # ------------------------------
    if table_name == 'ride_bookings':
        df.to_sql(
            name=table_name,
            con=pg_engine,
            schema=SCHEMA,
            if_exists="append",  # append, không replace
            index=False
        )
    else:
        pg_hook.run(f"TRUNCATE TABLE {SCHEMA}.{table_name};")
        df.to_sql(
            name=table_name,
            con=pg_engine,
            schema=SCHEMA,
            if_exists="append",
            index=False
        )

    print(f"✅ Successfully ingested {len(df)} new records into {SCHEMA}.{table_name}")


# ------------------------------
# DAG DEFINITION
# ------------------------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
}

with DAG(
    dag_id="mysql_to_postgres_bronze_incremental",
    default_args=default_args,
    schedule_interval=None,  # chỉ chạy khi trigger thủ công
    catchup=False,
    description="Incremental ETL MySQL -> Postgres (Bronze layer)"
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
