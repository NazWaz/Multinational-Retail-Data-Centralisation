#%%
import yaml    
import sqlalchemy
from sqlalchemy import inspect
from sqlalchemy import text
import psycopg2
import pandas as pd
import numpy as np
#%%

class DataCleaning:

    def __init__(self):
        pass
    
    # cleans user data
    def clean_user_data(self):
        from data_extraction import DataExtractor
        extractor = DataExtractor()
        user_data = extractor.read_rds_table("legacy_users")
        # replaces NULL with NaN and drops
        clean_user_data = user_data.replace("NULL", np.nan)
        clean_user_data = clean_user_data.dropna()
        # creates mask to filter countries
        country = ["United Kingdom", "Germany", "United States"]
        mask = clean_user_data["country"].isin(country)
        clean_user_data = clean_user_data[mask]
        # replaces GGB with GB
        clean_user_data = clean_user_data.replace("GGB", "GB")
        # puts dates into correct format
        clean_user_data["date_of_birth"] = pd.to_datetime(clean_user_data["date_of_birth"], format="mixed")
        clean_user_data["join_date"] = pd.to_datetime(clean_user_data["join_date"], format="mixed")

        return clean_user_data


if __name__ == "__main__":
    cleaning = DataCleaning()
    clean_user_data = cleaning.clean_user_data()
    display(clean_user_data)
# %%
