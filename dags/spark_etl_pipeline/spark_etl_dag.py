
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id='spark_etl_pipeline',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    spark_etl_task = SparkSubmitOperator(
        task_id='spark_etl_job',
        conn_id='spark_default',
        application='/Users/Zhuanz/pythonTest/scu-2025JD/src/scripts/spark_etl_dag.py',
        verbose=False
    )
