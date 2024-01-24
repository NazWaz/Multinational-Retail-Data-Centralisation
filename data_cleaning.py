#%%
import yaml    
import sqlalchemy
from sqlalchemy import inspect
from sqlalchemy import text
import psycopg2
import pandas as pd
#%%

class DataCleaning:

    def __init__(self):
        pass
    
    # cleans user data
    def clean_user_data(self):
        from data_extraction import DataExtractor
        extractor = DataExtractor()
        user_data = extractor.read_rds_table("legacy_users")
        #display(user_data.info())
        #user_data.first_name = user_data.first_name.astype("string")
        #user_data.last_name = user_data.last_name.astype("string")
        #display(user_data.info())
        return user_data


if __name__ == "__main__":
    cleaning = DataCleaning()
    user_data = cleaning.clean_user_data()
    display(user_data)
# %%
