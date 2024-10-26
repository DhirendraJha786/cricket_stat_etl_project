# To initiate the dag Object
from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.python import PythonOperator  # Import PythonOperator
from google.cloud import storage
import requests
import csv

# Import Airflow Operator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor


# Define Constants
PROJECT_ID = 'burner-dhijha'
BUCKET_NAME = 'cricket_stat_bucket'
DATASET_NAME = 'cricket_dataset'
TABLE_NAME = 'ranking_table'

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
    querystring = {"formatType": "test"}
    headers = {
        "x-rapidapi-key": "71e79e18f4mshfcd9c2344551734p1ee8cfjsnc66d0421c81e",
        "x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    if response.status_code == 200:
        data = response.json().get('rank', [])
        csv_filename = 'batsmen_rankings.csv'

        if data:
            field_names = ['rank', 'name', 'country']

            # Write data to CSV file
            with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=field_names)
                for entry in data:
                    writer.writerow({field: entry.get(field) for field in field_names})

            print(f"Data fetched successfully and written to '{csv_filename}'")

            # Upload the CSV file to GCS
            bucket_name = 'cricket_stat_bucket'
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
    'fetch_cricket_stats',
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

    # Calling API and Fetching Data to GCS
    Fetch_Data_to_GCS = PythonOperator(
        task_id='fetch_and_upload_cricket_stats',
        python_callable=fetch_and_upload_cricket_stats,
    )
    
    # List Stat Bucket
    List_Bucket_files = GCSListObjectsOperator(
        task_id="List_Bucket_files",
        bucket=BUCKET_NAME,
        #gcp_conn_id=google_cloud_conn_id,
    )


    # Task Dependency
    Bq_Table_Exist >> Fetch_Data_to_GCS >> List_Bucket_files 