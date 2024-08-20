import dotenv
import os
dotenv.load_dotenv()
import custom_utils
import pymongo
import pandas as pd
import psycopg2
from sqlalchemy import create_engine


class MongoUtils:
    def __init__(self):
        try:
            self.mongo_db = os.getenv('MONGO_DB')
            self.mongo_collection = os.getenv('MONGO_COLLECTION')
            self.connection_string = os.getenv('MONGO_CONNECTION_STRING')
            
            if not self.mongo_db or not self.mongo_collection or not self.connection_string:
                raise ValueError("Missing one or more MongoDB environment variables.")
        except Exception as e:
            print(f"Error initializing MongoUtils: {e}")
            raise
    
    def get_mongo_conn(self):
        try:
            return pymongo.MongoClient(self.connection_string)
        except Exception as e:
            print(f"Error connecting to MongoDB: {e}")
            raise


def get_mongo_conn():
    try:
        connection_string = os.getenv('MONGO_CONNECTION_STRING')
        mongo_collection = os.getenv('MONGO_COLLECTION')
        mongo_db = os.getenv('MONGO_DB')
        
        if not connection_string or not mongo_collection or not mongo_db:
            raise ValueError("Missing one or more MongoDB environment variables.")
        
        return pymongo.MongoClient(connection_string)
    except Exception as e:
        print(f"Error getting MongoDB connection: {e}")
        raise


def get_postgres_conn(use_service: bool = True):
    try:
        if use_service:
            connection = psycopg2.connect(service='alt_data_db')
            return connection
        else:
            from custom_utils.onepassword_wrapper import OnePasswordWrapper
            wrapper = OnePasswordWrapper()
            item = wrapper.get_item_by_substring('aws-db')
            item.rich_print()
            server = item.server
            port = item.port
            username = item.username
            password = item.password
            connection = psycopg2.connect(
                host=server,
                port=port,
                user=username,
                password=password,
                dbname='alt_data'
            )
        return connection
    except Exception as e:
        print(f"Error getting PostgreSQL connection: {e}")
        raise
    

def get_aws_psql_conn():
    # Connection parameters
    dbname = 'alt_data'
    user = os.getenv('MONGO_USERNAME')
    password = os.getenv('MONGO_PASSWORD')
    host = 'alt-data.cklpdqecqefc.us-east-2.rds.amazonaws.com'  # This is the endpoint of your RDS instance
    # Connect to the database
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
    return conn


def get_local_psql_conn():
    engine = create_engine('postgresql://darien:@localhost:5432/alt_data')
    return engine.connect().connection