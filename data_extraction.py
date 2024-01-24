#%%
import yaml    
import sqlalchemy
from sqlalchemy import inspect
from sqlalchemy import text
import psycopg2
import pandas as pd

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

if __name__ == "__main__":
    extractor = DataExtractor()
    user_data = extractor.read_rds_table("legacy_users")
    display(user_data)
# %%
