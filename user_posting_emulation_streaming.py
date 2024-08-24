import yaml
import json
import requests
from time import sleep
import random
import sqlalchemy
from sqlalchemy import text
import boto3
from botocore.exceptions import ClientError


random.seed(100)


class AWSDBConnector:

    def __init__(self, creds_file='db_creds.yaml'):
        # Load database credentials from the YAML file
        with open(creds_file, 'r') as file:
            creds = yaml.safe_load(file)
        
        # Assign the credentials to instance variables
        self.HOST = creds['db']['HOST']
        self.USER = creds['db']['USER']
        self.PASSWORD = creds['db']['PASSWORD']
        self.DATABASE = creds['db']['DATABASE']
        self.PORT = creds['db'].get('PORT', 3306)

    def create_db_connector(self):
        # Create the SQLAlchemy engine using the credentials
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine

# Instantiate the connector
new_connector = AWSDBConnector()

def send_to_kinesis(stream_name, data):
    kinesis_client = boto3.client('kinesis')
    try:
        response = kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey=str(random.randint(1, 1000))
        )
        print(f"Successfully sent data to {stream_name}: {response}")
    except ClientError as e:
        print(f"Failed to send data to Kinesis stream {stream_name}: {e}")

def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            print(pin_result)
            print(geo_result)
            print(user_result)

            # Define Kinesis stream names
            pin_stream_name = "pinterest-data-stream"
            geo_stream_name = "geolocation-data-stream"
            user_stream_name = "user-data-stream"
            
            # Send data to Kinesis streams
            send_to_kinesis(pin_stream_name, pin_result)
            send_to_kinesis(geo_stream_name, geo_result)
            send_to_kinesis(user_stream_name, user_result)

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Streaming data to Kinesis')