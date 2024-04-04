#%%
import yaml    
import sqlalchemy
from sqlalchemy import inspect
from sqlalchemy import text
import psycopg2
import pandas as pd

import tabula

# %%
class DataExtractor:

    def __init__(self):
        pass

    # extracts table using table name and returns pd dataframe     
    def read_rds_table(self, table_name):
        from database_utils import DatabaseConnector 
        dbsconnector = DatabaseConnector()
        source_engine = dbsconnector.init_db_engine()
        data = pd.read_sql_table(table_name, source_engine)
        return data
    

    # takes in link and returns pandas DataFrame
    def retrieve_pdf_data(self, link):
        card_data_list = tabula.read_pdf(link, pages="all")
        card_data = pd.concat(card_data_list)
        return card_data





if __name__ == "__main__":
    extractor = DataExtractor()
    #user_data = extractor.read_rds_table("legacy_users")
    #display(user_data)
    #link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    #card_data = extractor.retrieve_pdf_data(link)
    #display(card_data)

# %%
