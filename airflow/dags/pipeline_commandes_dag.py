from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "data-engineer",
    "retries": 1,
}

with DAG(
    dag_id="pipeline_commandes_batch",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["kafka", "spark", "postgres"],
) as dag:

    clean_silver_task = BashOperator(
        task_id="clean_silver_dir",
        bash_command="rm -rf /opt/data/silver/commandes",
    )

    silver_task = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            "spark-submit --master local[2] "
            "/opt/spark_jobs/batch_bronze_to_silver.py"
        ),
    )

    gold_task = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            "spark-submit --master local[2] "
            "--packages org.postgresql:postgresql:42.7.3 "
            "/opt/spark_jobs/batch_silver_to_gold.py"
        ),
    )

    clean_silver_task >> silver_task >> gold_task