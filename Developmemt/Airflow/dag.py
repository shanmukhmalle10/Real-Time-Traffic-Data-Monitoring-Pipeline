from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime
import boto3

# Default args
default_args = {
    "owner": "data_engineer",
    "retries": 1
}

# Streams and corresponding firehose names
streams = [
    "incident-stream",
    "sensor-stream",
    "signal-stream",
    "speed-stream",
    "vehicle-stream"
]

stream_firehose_map = {
    "incident-stream": "incident-firehose",
    "sensor-stream": "sensor-firehose",
    "signal-stream": "signal-firehose",
    "speed-stream": "speed-firehose",
    "vehicle-stream": "vehicle-firehose"
}

# AWS configuration
AWS_REGION = "ap-south-1"
FIREHOSE_ROLE_ARN = "arn:aws:iam::071378139573:role/firehosedatabricks"
S3_BUCKET_ARN = "arn:aws:s3:::traffic-parquet-bucket"

# Functions to create Kinesis stream
def create_kinesis(stream_name, region_name=AWS_REGION):
    client = boto3.client("kinesis", region_name=region_name)
    existing_streams = client.list_streams()["StreamNames"]
    if stream_name not in existing_streams:
        client.create_stream(StreamName=stream_name, ShardCount=1)
        print(f"Kinesis stream {stream_name} created")
    else:
        print(f"Kinesis stream {stream_name} already exists")

# Function to create Firehose stream
def create_firehose(stream_name, delivery_stream_name, region_name=AWS_REGION):
    client = boto3.client("firehose", region_name=region_name)
    existing_streams = client.list_delivery_streams()["DeliveryStreamNames"]
    if delivery_stream_name not in existing_streams:
        client.create_delivery_stream(
            DeliveryStreamName=delivery_stream_name,
            S3DestinationConfiguration={
                "RoleARN": FIREHOSE_ROLE_ARN,
                "BucketARN": S3_BUCKET_ARN,
                "Prefix": f"{stream_name}/"
            }
        )
        print(f"Firehose delivery stream {delivery_stream_name} created")
    else:
        print(f"Firehose delivery stream {delivery_stream_name} already exists")

# DAG definition
with DAG(
    dag_id="DTL_Real_Time_traffic_data_monitoring",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    # Wait for files in S3
    wait_for_s3_files = S3KeySensor(
        task_id="wait_for_s3_files",
        bucket_name="traffic-raw-bucket",
        bucket_key="traffic_*.csv",
        wildcard_match=True,
        aws_conn_id="airflow",
        poke_interval=60,
        timeout=600
    )

    kinesis_tasks = []
    firehose_tasks = []

    # Create Kinesis and Firehose streams using PythonOperator
    for stream in streams:

        k_task = PythonOperator(
            task_id=f"create_kinesis_{stream}",
            python_callable=create_kinesis,
            op_args=[stream]
        )

        f_task = PythonOperator(
            task_id=f"create_firehose_{stream}",
            python_callable=create_firehose,
            op_args=[stream, stream_firehose_map[stream]]
        )

        k_task >> f_task

        kinesis_tasks.append(k_task)
        firehose_tasks.append(f_task)

    # Run Databricks job
    run_databricks = DatabricksRunNowOperator(
        task_id="run_databricks",
        databricks_conn_id="Databricks",
        job_id=480815953323162
    )

    # Chain tasks
    wait_for_s3_files >> kinesis_tasks
    for f in firehose_tasks:
        f >> run_databricks