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

    def read_rds_table(self):
        from database_utils import DatabaseConnector 
        dbsconnector = DatabaseConnector()
        source_engine = dbsconnector.init_db_engine()
        user_data = pd.read_sql_table("legacy_users", source_engine)
        return user_data

if __name__ == "__main__":
    extractor = DataExtractor()
    extractor.read_rds_table()

# %%
