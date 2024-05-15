#%%
import yaml    
import sqlalchemy
from sqlalchemy import inspect
from sqlalchemy import text
import psycopg2
import pandas as pd

#%%
class DatabaseConnector:

    def __init__(self):
        pass

    # reads database credentials    
    def read_db_creds(self):
        with open("db_creds.yaml", "r") as db_creds:
            db_creds = yaml.safe_load(db_creds)
        db_creds = list(db_creds.values())
        return db_creds
        
    # intialises sqlalchemy database engine
    def init_db_engine(self):
        db_creds = self.read_db_creds()
        HOST, PASSWORD, USER, DATABASE, PORT = [db_creds[i] for i in (0, 1, 2, 3, 4)] 
        source_engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return source_engine

    # lists all tables in database
    def list_db_tables(self):
        source_engine = self.init_db_engine()
        inspector = inspect(source_engine)
        print(inspector.get_table_names())

    # uploads data from pd to database in a table
    def upload_to_db(self, data,  table_name):
        db_creds = self.read_db_creds()
        
        HOST, PASSWORD, USER, DATABASE, PORT = [db_creds[i] for i in (5, 6, 7, 8, 9)] 
        sink_engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        data.to_sql(table_name, sink_engine, if_exists="replace")
       
if __name__ == "__main__":
    dbsconnector = DatabaseConnector()
    #dbsconnector.read_db_creds()
    #print(dbsconnector.read_db_creds())
    #dbsconnector.init_db_engine()
    #print(dbsconnector.init_db_engine())
    #dbsconnector.list_db_tables()
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

# %%
