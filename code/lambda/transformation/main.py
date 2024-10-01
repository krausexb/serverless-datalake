import json
import os
import boto3
from datetime import datetime
import pandas as pd
import numpy as np
import sqlite3

s3 = boto3.client('s3')

def convert(local_file):
    print(f"Processing file: {local_file}")
    
    db = sqlite3.connect("{}".format(local_file))
    cursor = db.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    table_name = cursor.fetchall()[0][0]
    df = pd.read_sql("SELECT * FROM {}".format(table_name) , db, parse_dates={"Timestamp"})
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df['Timestamp'] = df['Timestamp'].dt.strftime('%Y-%m-%d %X')
    df.to_csv("{}-converted.csv".format(local_file), index=False)

def process(event):
    print(event)
    input_bucket = os.environ.get("RAW_BUCKET_NAME")
    output_bucket = os.environ.get("PROCESSED_BUCKET_NAME")
    items = event['Items']
    tmp_dir = '/tmp/'
    tmp_files = os.listdir(tmp_dir)
    
    print(f"Files in tmp folder: {tmp_files}")
    
    for item in items:
        local_file = tmp_dir + item['Key']
        if not local_file.endswith("/"):
            print(f"Downloading Key {item['Key']} to {local_file}")
            s3.download_file(input_bucket, item['Key'], local_file)
            
            convert(local_file)

            file_basename = os.path.basename(local_file)
            file_partition_types = ["Hubbox_Sensordata", "Towerbox_Sensordata", "SCADA_Data"]

            for file_type in file_partition_types:
                if file_basename.startswith(file_type):
                    s3.upload_file(local_file + "-converted.csv", output_bucket, file_type + "/" + item['Key'] + "-converted.csv")
            
            os.remove(local_file)
        else:
            if not os.path.exists(local_file):
                os.makedirs(local_file)

    return f"Processed items: {items}"

def lambda_handler(event, context):
    try:
        response = process(event)
        return {
            'statusCode': 200,
            'body': json.dumps(f'Processed event: {str(response)}')
        }
    except Exception as e:
        print(f'An unexpected error occurred: {str(e)}')
        raise e
