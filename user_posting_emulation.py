import yaml
import pandas as pd
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


def save_table_to_csv(table_name, engine, file_name):
    # Fetch all data from the specified table
    query = text(f"SELECT * FROM {table_name}")
    with engine.connect() as connection:
        df = pd.read_sql_query(query, connection)
        # Save the data to a CSV file
        df.to_csv(file_name, index=False)
        print(f"{table_name} saved to {file_name}")

def run_post_data_loop():
    engine = new_connector.create_db_connector()

    # Save the tables as CSV files
    save_table_to_csv('pinterest_data', engine, 'pinterest_data.csv')
    save_table_to_csv('geolocation_data', engine, 'geolocation_data.csv')
    save_table_to_csv('user_data', engine, 'user_data.csv')

if __name__ == "__main__":
    run_post_data_loop()
    print('CSV files have been created.')