import yaml
import json
import requests
from time import sleep
import random
import sqlalchemy
from sqlalchemy import text


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

            # Define API Invoke URL for topics
            pin_invoke_url = "https://4m0a2wpt0f.execute-api.us-east-1.amazonaws.com/dev/topics/12c7b456b441.pin"
            geo_invoke_url = "https://4m0a2wpt0f.execute-api.us-east-1.amazonaws.com/dev/topics/12c7b456b441.geo"
            user_invoke_url = "https://4m0a2wpt0f.execute-api.us-east-1.amazonaws.com/dev/topics/12c7b456b441.user"

            # Prepare data for sending
            pin_payload = json.dumps({
                "records": [
                    {
                        "value": pin_result
                    }
                ]
            }, default=str)

            geo_payload = json.dumps({
                "records": [
                    {
                        "value": geo_result
                    }
                ]
            }, default=str)

            user_payload = json.dumps({
                "records": [
                    {
                        "value": user_result
                    }
                ]
            }, default=str)

            # Send data to the Kafka topics using API Invoke URL
            headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
            pin_response = requests.request("POST", pin_invoke_url, headers=headers, data=pin_payload)
            gro_response = requests.request("POST", geo_invoke_url, headers=headers, data=geo_payload)
            user_response = requests.request("POST", user_invoke_url, headers=headers, data=user_payload)
            print(pin_response.status_code)
            print(gro_response.status_code)
            print(user_response.status_code)

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')