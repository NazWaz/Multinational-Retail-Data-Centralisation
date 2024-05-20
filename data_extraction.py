import yaml    
import sqlalchemy
from sqlalchemy import inspect
from sqlalchemy import text
import psycopg2
import pandas as pd
import tabula
import requests
class DataExtractor:
    '''
    Extracts data from various sources.

    Methods:
    ------
    read_rds_table
        Extracts table from an AWS RDS database using table name to return a dataframe.
    retrieve_pdf_data
        Extracts card data from a PDF stored in an AWS S3 bucket using link to return a dataframe.
    list_number_of_stores
        Extracts number of stores from an API using the number of stores endpoint.
    retrieve_stores_data
        Extracts all the stores from an API using the number of stores and the retrieve store endpoint to return a dataframe. 
    extract_from_s3
        Extracts products CSV from an AWS S3 bucket using S3 address to return a dataframe.
    extract_json_from_s3
        Extracts date events JSON stored in an AWS S3 bucket using S3 address to return a dataframe.
    '''
    def __init__(self):
        pass
 
    def read_rds_table(self, table_name):
        '''
        Creates connection using an sqlalchemy database engine to extract and return a dataframe for a table from an AWS RDS database.
        '''

        from database_utils import DatabaseConnector 
        dbsconnector = DatabaseConnector()
        source_engine = dbsconnector.init_db_engine()
        data = pd.read_sql_table(table_name, source_engine)
        return data
    
    def retrieve_pdf_data(self, link):
        '''
        Extracts all pages of card data from a pdf stored in an AWS S3 bucket to return a dataframe.
        '''

        card_data_list = tabula.read_pdf(link, multiple_tables=True, pages="all", lattice=True)
        card_data = pd.concat(card_data_list)
        return card_data
    
    def list_number_of_stores(self, no_of_stores, headers):
        '''
        Extracts JSON from an API to return the number of stores to extract.
        '''

        no_of_stores_response = requests.get(no_of_stores, headers=headers)
        no_of_stores = no_of_stores_response.json()["number_stores"]
        return no_of_stores

    def retrieve_stores_data(self, retrieve_store, headers):
        '''
        Extracts dictionary of stores from an API using a for loop to iterate through the number of stores to return a dataframe.
        '''

        no_of_stores = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
        no_of_stores = self.list_number_of_stores(no_of_stores, headers)
        # empty list for dictionaries from data
        store_data = []
        # loops through each number of store from 0 to 450
        for store_number in range(0, no_of_stores):
            store = retrieve_store + f"{store_number}"
            store_data_response = requests.get(store, headers=headers)
            data = store_data_response.json()
            store_data.append(data)
        store_data = pd.DataFrame(store_data)
        return store_data
    
    def extract_from_s3(self, s3_address):
        '''
        Extracts product CSV from an AWS S3 bucket using S3 address to return a dataframe.
        '''
        
        products_data = pd.read_csv(s3_address, index_col=0)
        return products_data
    
    def extract_json_from_s3(self, s3_address):
        '''
        Extracts date events JSON stored in an AWS S3 bucket using S3 address to return a dataframe.
        '''
        
        events_data = pd.read_json(s3_address)
        return events_data

if __name__ == "__main__":
    extractor = DataExtractor()