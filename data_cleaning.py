#%%
import yaml    
import sqlalchemy
from sqlalchemy import inspect
from sqlalchemy import text
import psycopg2
import pandas as pd
import numpy as np
import regex
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

    # cleans store data
    def clean_store_data(self):
        from data_extraction import DataExtractor
        extractor = DataExtractor()
        headers = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
        retrieve_store = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
        store_data = extractor.retrieve_stores_data(retrieve_store, headers)

        # creates mask to filter continents
        continent = ["Europe", "America"]
        mask = store_data["continent"].isin(continent)
        clean_store_data = store_data[mask]
        # replaces eeEurope with Europe
        clean_store_data = clean_store_data.replace("eeEurope", "Europe")
        # drop lat column
        clean_store_data = clean_store_data.drop("lat", axis=1)
        # replace new lines in address column
        clean_store_data["address"] = clean_store_data["address"].str.replace("\n", " ")
        # puts dates into correct format
        clean_store_data["opening_date"] = pd.to_datetime(clean_store_data["opening_date"], format="mixed")
        # remove letters from staff numbers
        clean_store_data["staff_numbers"] = clean_store_data["staff_numbers"].str.replace("\D", "", regex=True)

        return clean_store_data


if __name__ == "__main__":
    cleaning = DataCleaning()
    clean_user_data = cleaning.clean_user_data()
    display(clean_user_data)
# %%
