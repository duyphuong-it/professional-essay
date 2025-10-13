import os
import re
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR = "/home/airflow/.dbt"

# Regex để bắt ref('...')
REF_REGEX = re.compile(r"ref\(['\"]([a-zA-Z0-9_]+)['\"]\)")

def parse_sql_dependencies(models_dir):
    dependencies = {}
    layers = {}
    for root, _, files in os.walk(models_dir):
        for file in files:
            if file.endswith(".sql"):
                model_name = os.path.splitext(file)[0]
                file_path = os.path.join(root, file)

                # Lấy layer (ví dụ: models/silver/model.sql → silver)
                rel_path = os.path.relpath(file_path, models_dir)
                parts = rel_path.split(os.sep)
                layer = parts[0] if len(parts) > 1 else "root"

                with open(file_path, "r") as f:
                    sql = f.read()
                    refs = REF_REGEX.findall(sql)

                dependencies[model_name] = refs
                layers[model_name] = layer

    return dependencies, layers


# Đọc dependency và layer từ thư mục models/
dependencies, layers = parse_sql_dependencies(os.path.join(DBT_PROJECT_DIR, "models"))

with DAG(
    dag_id="dbt_sql_parse_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    airflow_tasks = {}
    layer_groups = {}

    # Tạo TaskGroup cho từng layer
    unique_layers = set(layers.values())
    for layer in unique_layers:
        layer_groups[layer] = TaskGroup(group_id=f"build_{layer}", tooltip=f"DBT {layer} models")

    # Tạo task cho từng model trong layer tương ứng
    for model, refs in dependencies.items():
        layer = layers.get(model, "unknown")

        airflow_tasks[model] = BashOperator(
            task_id=f"dbt_run_{model}",
            bash_command=(
                f"dbt run --models {model} "
                f"--project-dir {DBT_PROJECT_DIR} "
                f"--profiles-dir {DBT_PROFILES_DIR}"
            ),
            task_group=layer_groups[layer],
        )

    # Set dependency theo ref()
    for model, refs in dependencies.items():
        for ref in refs:
            if ref in airflow_tasks:
                airflow_tasks[ref] >> airflow_tasks[model]

    # Đưa các group vào DAG (Airflow tự nhận nếu đã gán task_group)
    # Không cần thêm gì ở đây

    # Thiết lập thứ tự layer tổng quát (nếu muốn ép luồng chạy)
    if all(layer in layer_groups for layer in ["silver", "gold"]):
        layer_groups["silver"] >> layer_groups["gold"]