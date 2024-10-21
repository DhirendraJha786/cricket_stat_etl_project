import functions_framework
from googleapiclient.discovery import build

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def trigger_df_job(cloud_event):   
 
    service = build('dataflow', 'v1b3')
    project = "burner-dhijha"

    template_path = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"

    template_body = {
        "jobName": "gcs_to_bq_cricket_stat",
        "environment": {
            "numWorkers": 2,
            "workerRegion": "us-central1",
            "serviceAccountEmail": "514602624291-compute@developer.gserviceaccount.com",
            "tempLocation": "gs://dataflow_metadata_786/temp",
            "subnetwork": "regions/us-central1/subnetworks/subnet-01",
            "network": "dev-network",
            "ipConfiguration": "WORKER_IP_PRIVATE",
            "additionalExperiments": [
            "use_runner_v2"
            ],
            "additionalUserLabels": {}
        },
        "parameters": {
            "inputFilePattern": "gs://cricket_stat_bucket/batsmen_rankings.csv",
            "JSONPath": "gs://dataflow_metadata_786/bq.json",
            "outputTable": "burner-dhijha:cricket_dataset.ranking_table",
            "bigQueryLoadingTemporaryDirectory": "gs://dataflow_metadata_786",
            "javascriptTextTransformGcsPath": "gs://dataflow_metadata_786/udf.js",
            "javascriptTextTransformFunctionName": "transform"
        }
    }

    request = service.projects().templates().launch(projectId=project,gcsPath=template_path, body=template_body)
    response = request.execute()
    print(response)