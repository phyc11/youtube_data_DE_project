from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from youtube_elt.extract.extract_youtube_channel import extract_youtube_channel_data

default_args = {
    'owner': 'Phyc',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'youtube_channel_elt_dag',
    default_args=default_args,
    description='YouTube Channel ELT DAG',
    start_date=days_ago(1),
    schedule_interval="0 12 * * *",  
    catchup=False,
)

extract_channel_data_task = PythonOperator(
    task_id='extract_youtube_channel_data',
    python_callable=extract_youtube_channel_data,
    dag=dag,
)

load_channel_data_task = SparkSubmitOperator(
    task_id='load_youtube_channel_data',
    application='/opt/airflow/dags/youtube_elt/load/load_youtube_channel.py',
    conn_id='my_spark_config',
    packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.hadoop:hadoop-common:3.3.6,org.apache.hadoop:hadoop-hdfs:3.3.6",
    verbose=True,
    dag=dag,
)

transform_channel_data_task = SparkSubmitOperator(
    task_id='transform_youtube_channel_data',
    application='/opt/airflow/dags/youtube_elt/transform/transform_youtube_channel.py',
    conn_id='my_spark_config',
    packages="org.apache.hadoop:hadoop-common:3.3.6,org.apache.hadoop:hadoop-hdfs:3.3.6,org.apache.spark:spark-hive_2.12:3.4.0,org.postgresql:postgresql:42.6.0",
    verbose=True,
    dag=dag,
)

extract_channel_data_task >> load_channel_data_task >> transform_channel_data_task
