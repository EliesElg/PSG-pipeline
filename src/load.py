from requests import Response
from botocore.exceptions import BotoCoreError
import os
import json
import requests
from dotenv import load_dotenv
import boto3
import botocore
from datetime import datetime
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

def from_s3_to_bq(date_dag):
    # jme connecte a aws

    S3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
    )
    # recup data
    try:
        response = S3_client.get_object(Bucket=os.getenv("BUCKET_NAME"), Key=f"psg_matches_{date_dag}.json")
        data_to_insert = response['Body'].read().decode('utf-8')
    except botocore.exceptions.ClientError as e:
        print(f"An error occurred: {e}")

    os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    
    project_name=os.getenv("GOOGLE_PROJECT")
    dataset_name=os.getenv("GOOGLE_DATASET")
    table_name=os.getenv("GOOGLE_TABLE")

    table_id=f"{project_name}.{dataset_name}.{table_name}"

    schema = [
    bigquery.SchemaField("ingestion_date", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("file_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("raw_content", "JSON", mode="NULLABLE"), # "JSON" est le type magique
    ]

    bq_client = bigquery.Client()
    dataset = bq_client.dataset(dataset_name)
    table_ref = dataset.table(table_name) # Juste l'adresse (Pointeur)

    try:
        bq_client.get_table(table_id)
        print('Table existe déjà')
    except NotFound:
        print('Table existe pas, on la crée')
        # On construit l'objet Complet avec le Schéma pour la création
        table = bigquery.Table(table_id, schema=schema)
        bq_client.create_table(table)

    rows_to_insert= [{
        "ingestion_date": datetime.now().isoformat(),
        "file_name": f"psg_matches_{date_dag}.json",
        "raw_content": data_to_insert
        }]

    errors = bq_client.insert_rows_json(table_id,rows_to_insert)
    
    if errors == []:
        print('new rows added')
    else:
        print("errors: {}".format(errors))


