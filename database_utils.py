import yaml    
import sqlalchemy
from sqlalchemy import inspect
from sqlalchemy import text
import psycopg2
import pandas as pd
class DatabaseConnector:
    '''
    Connects with and uploads data to a postgresql database.

    Methods:
    -------
    read_db_creds
        Reads database credentials.
    init_db_engine
        Initialises sqlalchemy database engine.
    list_db_tables
        Lists all tables in database.
    upload_to_db
        Uploads data from dataframe to a table in a postgresql database.
    '''
    def __init__(self):
        pass
  
    def read_db_creds(self):
        '''
        Reads a credential yaml file to return a dictionary of the database credentials.
        '''

        with open("db_creds.yaml", "r") as db_creds:
            db_creds = yaml.safe_load(db_creds)
        db_creds = list(db_creds.values())
        return db_creds
        
    def init_db_engine(self):
        '''
        Reads the database credentials to initialise and return a sqlalchemy database engine. 
        '''

        db_creds = self.read_db_creds()
        HOST, PASSWORD, USER, DATABASE, PORT = [db_creds[i] for i in (0, 1, 2, 3, 4)] 
        source_engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return source_engine

    def list_db_tables(self):
        '''
        Lists all the tables in the database using the sqlalchemy database engine.
        '''

        source_engine = self.init_db_engine()
        inspector = inspect(source_engine)
        print(inspector.get_table_names())

    def upload_to_db(self, data,  table_name):
        '''
        Uploads data from dataframe to store in postgresql database using more database credentials from yaml file.
        '''

        db_creds = self.read_db_creds()
        HOST, PASSWORD, USER, DATABASE, PORT = [db_creds[i] for i in (5, 6, 7, 8, 9)] 
        sink_engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        data.to_sql(table_name, sink_engine, if_exists="replace")
       
if __name__ == "__main__":
    dbsconnector = DatabaseConnector()
    from data_cleaning import DataCleaning
    cleaning = DataCleaning()

    # uploads user data
    clean_user_data = cleaning.clean_user_data()
    dbsconnector.upload_to_db(clean_user_data, "dim_users")

    # uploads card data
    clean_card_data = cleaning.clean_card_data()
    dbsconnector.upload_to_db(clean_card_data, "dim_card_details")
    
    # uploads store data
    clean_store_data = cleaning.clean_store_data()
    dbsconnector.upload_to_db(clean_store_data, "dim_store_details")

    # uploads products data
    clean_products_data = cleaning.clean_products_data()
    dbsconnector.upload_to_db(clean_products_data, "dim_products")

    # uploads orders data
    clean_orders_data = cleaning.clean_orders_data()
    dbsconnector.upload_to_db(clean_orders_data, "orders_table")

    # uploads date events data
    clean_events_data = cleaning.clean_events_data()
    dbsconnector.upload_to_db(clean_events_data, "dim_date_times")
