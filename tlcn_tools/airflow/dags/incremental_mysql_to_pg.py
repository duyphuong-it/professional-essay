from airflow import DAG
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
    if table_name == "ride_bookings":
        try:
            query_last_ts = f"""
                SELECT MAX(event_timestamp) AS last_ts
                FROM {SCHEMA}.{table_name};
            """
            last_ts_df = pd.read_sql(query_last_ts, pg_engine)
            last_ts = last_ts_df["last_ts"][0]
        except Exception:
            last_ts = None

        if pd.isna(last_ts) or last_ts is None:
            last_ts = datetime(1900, 1, 1)
            print(f"⚙️ No existing data found for {table_name}, performing initial load.")
        else:
            print(f"⚙️ Last ingested timestamp for {table_name}: {last_ts}")
    else:
        last_ts = None  # các bảng khác full load

    # ------------------------------
    # 2️⃣ Lấy dữ liệu mới hơn từ MySQL
    # ------------------------------
    if table_name == "ride_bookings":
        query = f"""
            SELECT *
            FROM {table_name}
            WHERE event_timestamp > '{last_ts}';
        """
    else:
        query = f"SELECT * FROM {table_name};"

    df = mysql_hook.get_pandas_df(query)

    if df.empty:
        print(f"✅ No new data for table '{table_name}'. Skipping load.")
        return

    # ------------------------------
    # 3️⃣ Làm sạch dữ liệu (nếu cần)
    # ------------------------------
    if table_name == "ride_bookings":
        df["booking_id"] = df["booking_id"].astype(str).str.replace('"', '', regex=False)
        df["customer_id"] = df["customer_id"].astype(str).str.replace('"', '', regex=False)

        if "time" in df.columns and np.issubdtype(df["time"].dtype, np.timedelta64):
            df["time"] = df["time"].apply(lambda x: str(x).split()[-1] if pd.notnull(x) else None)

    # ------------------------------
    # 4️⃣ Append dữ liệu mới vào PostgreSQL
    # ------------------------------
    if table_name == "ride_bookings":
        if_exists_mode = "append"
    else:
        try:
            pg_hook.run(f"TRUNCATE TABLE {SCHEMA}.{table_name} RESTART IDENTITY;")
            if_exists_mode = "append"
        except Exception:
            if_exists_mode = "replace"
            
    df.to_sql(
        name=table_name,
        con=pg_engine,
        schema=SCHEMA,
        if_exists=if_exists_mode,
        index=False
    )

    # ------------------------------
    # 5️⃣ Tạo index cho event_timestamp trong PostgreSQL (nếu có cột này)
    # ------------------------------
    if table_name == "ride_bookings":
        create_index_sql = f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_indexes 
                    WHERE schemaname = '{SCHEMA}' 
                    AND tablename = '{table_name}'
                    AND indexname = '{table_name}_event_timestamp_idx'
                ) THEN
                    EXECUTE 'CREATE INDEX {table_name}_event_timestamp_idx 
                             ON {SCHEMA}.{table_name}(event_timestamp);';
                END IF;
            END $$;
        """
        pg_hook.run(create_index_sql)

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
    schedule_interval=None,
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
