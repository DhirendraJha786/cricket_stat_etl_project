# To initiate the dag Object
import datetime
import uuid
from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.python import PythonOperator  # Import PythonOperator
from google.cloud import storage
import requests
import csv

# Import Airflow Operator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator, GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator



# Define Constants
PROJECT_ID = 'burner-dhijha'
BUCKET_NAME = 'cricket_stat_bucket_odi'
DATASET_NAME = 'cricket_dataset'
TABLE_NAME = 'odi_ranking_table'

# Define Defaul Args
default_args = {
    'owner': 'Dhirendra',
    'start_date': datetime(2023, 12, 18),
    'depends_on_past': False,
    'email': ['dhirendravats786@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Function to call the API and Store file in GCS Bucket
def fetch_and_upload_cricket_stats():
    url = "https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/batsmen"
    querystring = {"formatType": "odi"}
    headers = {
        "x-rapidapi-key": "71e79e18f4mshfcd9c2344551734p1ee8cfjsnc66d0421c81e",
        "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    if response.status_code == 200:
        data = response.json().get('rank', [])
        csv_filename = 'batsmen_rankings_odi.csv'

        if data:
            field_names = ['rank', 'name', 'country']

            # Write data to CSV file
            with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=field_names)
                for entry in data:
                    writer.writerow({field: entry.get(field) for field in field_names})

            print(f"Data fetched successfully and written to '{csv_filename}'")

            # Upload the CSV file to GCS
            bucket_name = 'cricket_stat_bucket_odi'
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            destination_blob_name = f'{csv_filename}'

            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(csv_filename)

            print(f"File {csv_filename} uploaded to GCS bucket {bucket_name} as {destination_blob_name}")
        else:
            print("No data available from the API.")
    else:
        print("Failed to fetch data:", response.status_code)

# Define your DAG
dag = DAG(
    'fetch_cricket_stats_to_bq',
    default_args=default_args,
    description='Fetch cricket stats and upload to GCS',
    schedule_interval=None,
    catchup=False
)

with dag:
    # Check if table Exists
    Bq_Table_Exist = BigQueryTableExistenceSensor(
        task_id="Bq_Table_Exist",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
    )

    # Create Bucket to store Data
    Create_Stat_Bucket = GCSCreateBucketOperator(
        task_id="Create_Stat_Bucket",
        project_id=PROJECT_ID,
        bucket_name=BUCKET_NAME,
        storage_class="STANDARD",
        location="us-central1",
        labels={"env": "dev", "team": "airflow"},
        #gcp_conn_id="airflow-conn-id",
    )

    # List Stat Bucket
    List_Bucket_files = GCSListObjectsOperator(
        task_id="List_Bucket_files",
        bucket=BUCKET_NAME,
        #gcp_conn_id=google_cloud_conn_id,
    )


    # Calling API and Fetching Data to GCS
    Fetch_Data_to_GCS = PythonOperator(
        task_id='fetch_and_upload_cricket_stats',
        python_callable=fetch_and_upload_cricket_stats,
    )

    # Generate a unique job name using a timestamp
    dataflow_job_name = f"gcs_to_bq_cricket_stat_{uuid.uuid4()}"

    # Task to start a Dataflow job using a GCS template
    start_dataflow_job = DataflowTemplatedJobStartOperator(
        task_id='start_dataflow_gcs_to_bq',
        project_id='burner-dhijha',
        location='us-central1',
        template='gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery',
        job_name=dataflow_job_name,
        parameters={
            'inputFilePattern': 'gs://cricket_stat_bucket_odi/batsmen_rankings_odi.csv',
            'JSONPath': 'gs://dataflow_metadata_786/bq.json',
            'outputTable': 'burner-dhijha:cricket_dataset.odi_ranking_table',
            'bigQueryLoadingTemporaryDirectory': 'gs://dataflow_metadata_786',
            'javascriptTextTransformGcsPath': 'gs://dataflow_metadata_786/udf.js',
            'javascriptTextTransformFunctionName': 'transform',
        },
        environment={
            'numWorkers': 2,
            'workerRegion': 'us-central1',
            'serviceAccountEmail': '514602624291-compute@developer.gserviceaccount.com',
            'tempLocation': 'gs://dataflow_metadata_786/temp',
            'subnetwork': 'regions/us-central1/subnetworks/subnet-01',
            'network': 'dev-network',
            'ipConfiguration': 'WORKER_IP_PRIVATE',
            'additionalExperiments': ['use_runner_v2'],
            'additionalUserLabels': {},
        },
    )

    # Cleanup - Delete Stat Bucket
    Cleanup = GCSDeleteBucketOperator(
        task_id="Delete_Stat_Bucket",
        bucket_name=BUCKET_NAME,
        force=True,
        #gcp_conn_id="airflow-conn-id",
    )
    


    # Task Dependency
    Bq_Table_Exist >> Create_Stat_Bucket >> Fetch_Data_to_GCS >> List_Bucket_files >> start_dataflow_job >>  Cleanup






