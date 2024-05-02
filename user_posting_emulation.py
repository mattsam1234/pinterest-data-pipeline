import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import yaml
import sqlalchemy
from sqlalchemy import text
import datetime


random.seed(100)


class AWSDBConnector:
    '''
     A class to emulate users posting data

    ...

    Attributes
    ----------
    credentials = dictionary of credentials
    self.HOST = HOST credential value
    self.USER = USER credential value
    self.PASSWORD = PASSWORD credential value
    self.DATABASE = DATABASE credential value
    self.PORT = PORT credential value

    Methods
    -------
    read_db_creds = Method used to read a yaml file and return the credentials as a dictionary
    create_db_connector = Method to create an SQLAlchemy Engine object to connect to database given the parameters above
    '''
    def __init__(self):
        credentials = self.read_db_creds('db_creds.yaml')
        self.HOST = credentials['HOST']
        self.USER = credentials['USER']
        self.PASSWORD = credentials['PASSWORD']
        self.DATABASE = credentials['DATABASE']
        self.PORT = credentials['PORT']
        
    def read_db_creds(self, path_to_credentials):
        '''
        Use Yaml to load a credentials file.
        Parameters
        ----------
        path_to_credentials(str) : Path to the credentials file
        
        Returns 
        -------
        Loaded Credentials
        '''
        with open(path_to_credentials, 'r') as db_creds:
            loaded_creds = yaml.safe_load(db_creds)
        return loaded_creds
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()

def serialize_datetime(obj): 
    if isinstance(obj, datetime.datetime): 
        return obj.isoformat() 
    raise TypeError("Type not serializable") 


def run_infinite_post_data_loop():
    '''
    An infinitely running method to pull data from the the engine in previously created connector object and prints out 3 different types of data.
    '''
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            #pinsting
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
                
            pin_invoke_url = "https://rluf8oam8i.execute-api.us-east-1.amazonaws.com/dev/topics/0e5f67235f6b.pin"
            #To send JSON messages you need to follow this structure
            payload = json.dumps({
                "records": [
                    {
                    #Data should be send as pairs of column_name:value, with different columns separated by commas       
                    "value": pin_result
                    }
                ]
            }, default=serialize_datetime)
            headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
            pin_response = requests.request("POST", pin_invoke_url, headers=headers, data=payload)

            #geostring
            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            geo_invoke_url = "https://rluf8oam8i.execute-api.us-east-1.amazonaws.com/dev/topics/0e5f67235f6b.geo"
            #To send JSON messages you need to follow this structure
            payload = json.dumps({
                "records": [
                    {
                    #Data should be send as pairs of column_name:value, with different columns separated by commas       
                    "value": geo_result
                    }
                ]
            }, default=serialize_datetime)
            headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
            geo_response = requests.request("POST", geo_invoke_url, headers=headers, data=payload)
            
            #userstring
            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
                
            user_invoke_url = "https://rluf8oam8i.execute-api.us-east-1.amazonaws.com/dev/topics/0e5f67235f6b.user"
            #To send JSON messages you need to follow this structure
            payload = json.dumps({
                "records": [
                    {
                    #Data should be send as pairs of column_name:value, with different columns separated by commas       
                    "value": user_result
                    }
                ]
            }, default=serialize_datetime)
            headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
            user_response = requests.request("POST", user_invoke_url, headers=headers, data=payload)
            
            print(pin_result)
            print(pin_response)
            
            print(geo_result)
            print(geo_response)
            
            print(user_result)
            print(user_response)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')