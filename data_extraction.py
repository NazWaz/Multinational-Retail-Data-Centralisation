#%%
import yaml    
import sqlalchemy
from sqlalchemy import inspect
from sqlalchemy import text
import psycopg2
import pandas as pd

import tabula

import requests

# %%
class DataExtractor:

    def __init__(self):
        pass

    # extracts table using table name and returns pd DataFrame     
    def read_rds_table(self, table_name):
        from database_utils import DatabaseConnector 
        dbsconnector = DatabaseConnector()
        source_engine = dbsconnector.init_db_engine()
        data = pd.read_sql_table(table_name, source_engine)
        return data
    
    # takes in link and returns pd DataFrame
    def retrieve_pdf_data(self, link):
        card_data_list = tabula.read_pdf(link, pages="all")
        card_data = pd.concat(card_data_list)
        return card_data
    
    # returns number of stores to extract
    def list_number_of_stores(self, no_of_stores, headers):
        no_of_stores_response = requests.get(no_of_stores, headers=headers)
        no_of_stores = no_of_stores_response.json()["number_stores"]
        return no_of_stores

    # extracts all stores from API and saves them in pd DataFrame
    def retrieve_stores_data(self, retrieve_store, headers):
        no_of_stores = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
        no_of_stores = self.list_number_of_stores(no_of_stores, headers)
        # empty list for dictionaries from data
        store_data = []
        # loops through each number of store from 0 to 450
        for store_number in range(0, no_of_stores - 1):
            store = retrieve_store + f"{store_number}"
            store_data_response = requests.get(store, headers=headers)
            data = store_data_response.json()
            store_data.append(data)
        store_data = pd.DataFrame(store_data)
        return store_data

if __name__ == "__main__":
    extractor = DataExtractor()
    #user_data = extractor.read_rds_table("legacy_users")
    #display(user_data)
    #link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    #card_data = extractor.retrieve_pdf_data(link)
    #display(card_data)
    ##headers = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    #no_of_stores = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    #no_of_stores = extractor.list_number_of_stores(no_of_stores, headers)
    ##retrieve_store = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
    ##store_data = extractor.retrieve_stores_data(retrieve_store, headers)
   

#%%

    

# %%
