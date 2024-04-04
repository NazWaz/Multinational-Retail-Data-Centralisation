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
        # sets index value
        clean_user_data = user_data.set_index("index")
        # replaces NULL with NaN and drops
        clean_user_data = clean_user_data.replace("NULL", np.nan).dropna()
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


    # cleans card data
    def clean_card_data(self):
        from data_extraction import DataExtractor
        extractor = DataExtractor()
        link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        card_data = extractor.retrieve_pdf_data(link)
        # reset index values 
        clean_card_data = card_data.reset_index(drop=True)
        # converts data type to numeric, returning NaN if not possible
        clean_card_data["card_number"] = pd.to_numeric(clean_card_data["card_number"], errors="coerce")
        # replaces NULL with NaN and drops
        clean_card_data = clean_card_data.replace("NULL", np.nan).dropna()

        clean_card_data.card_number = clean_card_data.card_number.astype("int64")
        # puts dates into correct format
        clean_card_data["date_payment_confirmed"] = pd.to_datetime(clean_card_data["date_payment_confirmed"], format="mixed")

        

        return  clean_card_data



if __name__ == "__main__":
    cleaning = DataCleaning()
    clean_user_data = cleaning.clean_user_data()
    display(clean_user_data)
# %%
