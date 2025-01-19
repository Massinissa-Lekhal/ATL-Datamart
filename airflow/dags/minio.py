# Import Python dependencies needed for the workflow
from urllib import request
from minio import Minio, S3Error
from datetime import datetime
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pendulum
import os
import urllib.error

def download_parquet(**kwargs):
    # folder_path: str = r'..\..\data\raw'
    # Construct the relative path to the folder
    url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    filename: str = "yellow_tripdata"
    extension: str = ".parquet"

    month: str = pendulum.now().subtract(months=2).format('YYYY-MM')
    file_url = f"{url}{filename}_{month}{extension}"
    
    try:
        print(f"Attempting to download file from: {file_url}")
        request.urlretrieve(file_url, f"yellow_tripdata_{month}.parquet")
        print(f"File downloaded successfully: yellow_tripdata_{month}.parquet")
    
    except urllib.error.URLError as e:
        print(f"URLError occurred: {str(e)}")
        raise RuntimeError(f"Failed to download the parquet file from {file_url}: {str(e)}") from e
    
    except urllib.error.ContentTooShortError as e:
        print(f"ContentTooShortError occurred: {str(e)}")
        raise RuntimeError(f"Download incomplete for file: {file_url}. Content too short.") from e
    
    except Exception as e:
        print(f"Unexpected error occurred: {str(e)}")
        raise RuntimeError(f"An unexpected error occurred during the download: {str(e)}") from e


# Python Function
def upload_file(**kwargs):
    ###############################################
    # Upload generated file to Minio
    client = Minio(
        "minio:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = 'rawnyc'

    month: str = pendulum.now().subtract(months=2).format('YYYY-MM')
    
    try:
        # Vérification que le bucket existe
        print(f"Checking if bucket '{bucket}' exists...")
        if not client.bucket_exists(bucket):
            print(f"Bucket '{bucket}' does not exist. Creating bucket...")
            client.make_bucket(bucket)
        print(f"Bucket '{bucket}' exists or was created.")

        print(f"Attempting to upload file: yellow_tripdata_{month}.parquet")
        client.fput_object(
            bucket_name=bucket,
            object_name=f"yellow_tripdata_{month}.parquet",
            file_path=f"yellow_tripdata_{month}.parquet"
        )
        print(f"File uploaded to Minio: yellow_tripdata_{month}.parquet")
        
        # Supprimer le fichier local après l'upload
        os.remove(f"yellow_tripdata_{month}.parquet")
        print("Local file deleted after upload.")
    
    except S3Error as e:
        print(f"S3Error occurred: {str(e)}")
        raise RuntimeError(f"Failed to upload the file to Minio: {str(e)}") from e
    
    except Exception as e:
        print(f"Unexpected error occurred: {str(e)}")
        raise RuntimeError(f"An unexpected error occurred during the upload: {str(e)}") from e


###############################################
with DAG(dag_id='Grab_NYC_Data_to_Minio',
        start_date=days_ago(1),
        schedule_interval=None,
        catchup=False,
        tags=['minio/read/write'],
        ) as dag:
    ###############################################
    # Create a task to call your processing function
    t1 = PythonOperator(
        task_id='download_parquet',
        provide_context=True,
        python_callable=download_parquet
    )
    t2 = PythonOperator(
        task_id='upload_file_task',
        provide_context=True,
        python_callable=upload_file
    )
###############################################  

###############################################
# first upload the file, then read the other file.
t1 >> t2
############################################### 
