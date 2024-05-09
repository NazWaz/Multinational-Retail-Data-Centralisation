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
        # sets index value
        clean_store_data = store_data.set_index("index")
        # creates mask to filter continents
        continent = ["Europe", "America"]
        mask = clean_store_data["continent"].isin(continent)
        clean_store_data = clean_store_data[mask]
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
    
    @staticmethod
    def clean_weights(weight):
            
            if "kg" in weight:
                weight = weight.replace("kg", "")
                weight = float(weight)

            elif "x" in weight:
                weight = weight.replace("g", "")
                weight_list = weight.split(" x ")
                weight_list = [float(i) for i in weight_list]
                weight = weight_list[0] * weight_list[1]
                weight = weight/1000

            elif "g ." in weight:
                weight = weight.replace("g .", "")
                weight = float(weight)/1000
                
            elif "g" in weight:
                weight = weight.replace("g", "")
                weight = float(weight)/1000

            elif "ml" in weight:
                weight = weight.replace("ml", "")
                weight = float(weight)/1000

            elif "oz" in weight:

                weight = weight.replace("oz", "")
                weight = float(weight)*0.0283495231

            else:
                return weight

            return weight

    def convert_product_weights(self):
        from data_extraction import DataExtractor
        extractor = DataExtractor()
        s3_address = "s3://data-handling-public/products.csv"
        products_data = extractor.extract_from_s3(s3_address)

        clean_products_data = products_data.replace("NULL", np.nan).dropna()
        # creates mask to filter removed
        removed = ["Still_avaliable", "Removed"]
        mask = clean_products_data["removed"].isin(removed)
        clean_products_data = clean_products_data[mask]
        # corrects typo
        clean_products_data = clean_products_data.replace("Still_avaliable", "Still available")
        # casts data type of weight column to string
        clean_products_data["weight"] = clean_products_data["weight"].astype("string")
        # applies function to weight column and rounds to 3 d.p.
        clean_products_data["weight"] = clean_products_data["weight"].apply(self.clean_weights).round(3)
        
        return clean_products_data
    

    def clean_products_data(self):
        clean_products_data = self.convert_product_weights()
        # remove pound sign and convert data type of product price to float
        clean_products_data["product_price"] = clean_products_data["product_price"].str.replace("£", "").astype(float)
        # removes hyphens in category column
        clean_products_data["category"] = clean_products_data["category"].str.replace("-", " ")
        # puts date into correct format
        clean_products_data["date_added"] = pd.to_datetime(clean_products_data["date_added"], format="mixed")
        
        return clean_products_data
    
    def clean_orders_data(self):
        from data_extraction import DataExtractor
        extractor = DataExtractor()
        orders_data = extractor.read_rds_table("orders_table")
        # filters columns to remove first name, last name and 1
        clean_orders_data = orders_data[["index", "date_uuid", "user_uuid", "card_number", "store_code", "product_code", "product_quantity"]]
        # sets index
        clean_orders_data = clean_orders_data.set_index("index")

        return clean_orders_data
    
    def clean_events_data(self):
        from data_extraction import DataExtractor
        extractor = DataExtractor()
        s3_address = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
        events_data = extractor.extract_json_from_s3(s3_address)
        
        clean_events_data = events_data

        return clean_events_data

if __name__ == "__main__":
    cleaning = DataCleaning()
    clean_user_data = cleaning.clean_user_data()
    display(clean_user_data)
# %%
