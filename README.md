# Multinational-Retail-Data-Centralisation

This project involved extracting and cleaning data from multiple sources and sending this data into one centralised Postgresql database in pgAdmin4.


## Milestone 1

The first milestone was to set up the Github repository aswell as the dev environment. For this project several modules were imported including sqlalchemy, pandas, numpy, tabula and requests. 


## Milestone 2

The second milestone was to extract all of the necessary data from multiple sources and clean all of this data. First a database was set up within pgAdmin 4 called `sales_data` which was later used to store all the company information and data.

![](Documentation/2/1.png)

Then three python files were created `data_extraction.py`, `database_utils.py` and `data_cleaning.py` each with three separate classes intialised: `DataExtractor`, `DatabaseConnector` and `DataCleaning`.

![](Documentation/2/2.png)

- This class contained all the methods to help extract data from the different data sources.

![](Documentation/2/3.png)

- This class was used to connect with and upload data to the `sales_data` database.

![](Documentation/2/4.png)

- This class contained all the methods to clean the data from all of the sources.

# User data

The user data was the historical data of the users stored in an AWS database.

![](Documentation/2/5.png)

- A yaml file `db_creds.yaml` was created to contain all the database credentials containing the user data. This file was added to the `.gitignore` file so it wouldn't be visible publicly on Github. This method `read_db_creds` was used to read this yaml file and return the credentials as a dictionary.

![](Documentation/2/6.png)

- The `init_db_engine` method in the `DatabaseConnector` class was used to initialise and return a sqlalchemy database engine as the `source_engine` using the database credentials from earlier.

![](Documentation/2/7.png)

- The `list_db_tables` was used to extract and list all the table names present within this database.

![](Documentation/2/8.png)

- The `read_db_tables` method in the `DataExtractor` class was used to extract a database table to return a dataframe using a table name as an argument.

![](Documentation/2/9.png)

- The `clean_user_data` method in the `DataCleaning` class was used to perform the cleaning of the user data. The `legacy_users` table was used to extract the user data firstly. Then the index value was set to the index column using `DF.set_index("index")`.

- Any null values were replaced with NaN and dropped using `DF.replace("NULL", np.nan)` and `DF.dropna()`.

- A mask was created to filter valid countries, eliminating any irregular values in this column using `DF["column_name"].isin()` and then `DF[mask]`.

- The incorrect country code of `GGB` was replaced with `GB` using `DF.replace()`.

- Finally the dates were put into the correct format using `pd.to_datetime(DF["column_name"], format="mixed")`. This cast the column into the date time format while the `"mixed"` flag accounted for different date formats.

![](Documentation/2/10.png)

- The `upload_to_db` method was used to upload data to the `sales_data` database using new credentials from the yaml file aswell as the dataframe and desired table name within pgAdmin4 as arguments.  

![](Documentation/2/11.png)

- The `DatabaseConnector()` class was intialised and the `DataCleaning()` class was imported and also intialised to return the cleaned user data as `clean_user_data` and then upload this data to the database under the table name `dim_users`.

# Card data

The card data was the data of the users card details stored in a PDF in an AWS S3 bucket.

![](Documentation/2/12.png)

-

![](Documentation/2/13.png)

-

![](Documentation/2/14.png)

-

# Store data

The store data was the data of the store details and was extracted through the use of an API.

![](Documentation/2/15.png)

-

![](Documentation/2/16.png)

-

![](Documentation/2/17.png)

-

![](Documentation/2/18.png)

-

# Products data

The products data was the data of each product the company sells stored as a CSV in an S3 bucket.

![](Documentation/2/19.png)

-

![](Documentation/2/20.png)

-

![](Documentation/2/21.png)

-

![](Documentation/2/22.png)

-

![](Documentation/2/23.png)

-

# Orders data

The orders data was the data for all orders the company made stored in an AWS RDS. This table was to be the single source of truth and used to link all the tables later.

![](Documentation/2/24.png)

-

![](Documentation/2/25.png)

-

# Data events data

The data events data was the data of when each sale happened as well as related attributes and was stored as a JSON file.

![](Documentation/2/26.png)

-

![](Documentation/2/27.png)

-

![](Documentation/2/28.png)

-


## Milestone 3

This milestone was

![](Documentation/3/1.png)

-

![](Documentation/3/2.png)

-

![](Documentation/3/3.png)

-

![](Documentation/3/4.png)

-

![](Documentation/3/5.png)

-

![](Documentation/3/6.png)

-

![](Documentation/3/7.png)

-

![](Documentation/3/8.png)

-

![](Documentation/3/9.png)

-


## Milestone 4

This milestone was

![](Documentation/4/1.png)

-

![](Documentation/4/2.png)

-

![](Documentation/4/3.png)

-

![](Documentation/4/4.png)

-

![](Documentation/4/5.png)

-

![](Documentation/4/6.png)

-

![](Documentation/4/7.png)

-

![](Documentation/4/8.png)

-

![](Documentation/4/9.png)

-

![](Documentation/4/10.png)

-

![](Documentation/4/11.png)

-

![](Documentation/4/12.png)

-

![](Documentation/4/13.png)

-

![](Documentation/4/14.png)

-

![](Documentation/4/15.png)

-

![](Documentation/4/16.png)

-

![](Documentation/4/17.png)

-

![](Documentation/4/18.png)

-