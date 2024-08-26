import yaml
import json
import random
from time import sleep
import sqlalchemy
from sqlalchemy import text
import requests

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

def send_to_kinesis_via_http(url, headers, payload):
    try:
        response = requests.request("PUT", url, headers=headers, data=payload, timeout=30)
        response.raise_for_status()  # Raises an exception for HTTP errors
        print(f"Successfully sent data: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to send data: {e}")

def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            # Fetch Pinterest data
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_rows = connection.execute(pin_string)
            pin_result = {}
            for row in pin_selected_rows:
                pin_result = dict(row._mapping)

            # Fetch Geolocation data
            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_rows = connection.execute(geo_string)
            geo_result = {}
            for row in geo_selected_rows:
                geo_result = dict(row._mapping)

            # Fetch User data
            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_rows = connection.execute(user_string)
            user_result = {}
            for row in user_selected_rows:
                user_result = dict(row._mapping)

            # Convert datetime fields to ISO format
            if 'timestamp' in geo_result:
                geo_result['timestamp'] = geo_result['timestamp'].isoformat()
            if 'date_joined' in user_result:
                user_result['date_joined'] = user_result['date_joined'].isoformat()

            print(pin_result)
            print(geo_result)
            print(user_result)

            # Prepare HTTP requests to send data to Kinesis streams
            headers = {
                'Content-Type': 'application/json'
            }

            # Define Kinesis HTTP endpoint URLs for different streams
            pin_url = "https://4m0a2wpt0f.execute-api.us-east-1.amazonaws.com/dev/streams/streaming-12c7b456b441-pin/record"
            geo_url = "https://4m0a2wpt0f.execute-api.us-east-1.amazonaws.com/dev/streams/streaming-12c7b456b441-geo/record"
            user_url = "https://4m0a2wpt0f.execute-api.us-east-1.amazonaws.com/dev/streams/streaming-12c7b456b441-user/record"

            pin_payload = json.dumps({
                "StreamName": "streaming-12c7b456b441-pin",
                "Data": pin_result,
                "PartitionKey": "partition-1"
            })

            geo_payload = json.dumps({
                "StreamName": "streaming-12c7b456b441-geo",
                "Data": geo_result,
                "PartitionKey": "partition-2"
            })

            user_payload = json.dumps({
                "StreamName": "streaming-12c7b456b441-user",
                "Data": user_result,
                "PartitionKey": "partition-3"
            })

            # Send data to Kinesis streams via HTTP
            send_to_kinesis_via_http(pin_url, headers, pin_payload)
            send_to_kinesis_via_http(geo_url, headers, geo_payload)
            send_to_kinesis_via_http(user_url, headers, user_payload)

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Streaming data to Kinesis')
