#%%
import yaml    
import sqlalchemy
from sqlalchemy import inspect
from sqlalchemy import text
import psycopg2
import pandas as pd

# %%
# Reads yaml file
    
with open("db_creds.yaml", "r") as db_creds:
    db_creds = yaml.safe_load(db_creds)

print(type(db_creds))
print(db_creds)
# %%
# Creates sqlalchemy database engine
USER = db_creds["RDS_USER"]
PASSWORD = db_creds["RDS_PASSWORD"] 
HOST = db_creds["RDS_HOST"]
PORT = db_creds["RDS_PORT"]
DATABASE = db_creds["RDS_DATABASE"]

engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

# %%
# Lists all the tables in the database

#engine.connect()

inspector = inspect(engine)
inspector.get_table_names()

print(inspector.get_table_names())
# %%
# Extracts table containing user data to return pandas DataFrame
with engine.connect() as connection:
    user_data = connection.execute(text("SELECT * FROM legacy_users"))
    for row in user_data:
        print(row)

#%%
        
user_data = pd.read_sql_table("legacy_users", engine)
user_data.head(10)


#%%
# Cleans user data


#%%
# Uploads user data to "dim_users"
USER = "postgres"
PASSWORD = "Wazidur786"
HOST = "localhost"
PORT = 5432
DATABASE = "sales_data"

engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

user_data.to_sql("dim_users", engine, if_exists="replace")

# %%
USER = db_creds["USER"]
PASSWORD = db_creds["PASSWORD"] 
HOST = db_creds["HOST"]
PORT = db_creds["PORT"]
DATABASE = db_creds["DATABASE"]

engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

user_data.to_sql("dim_users", engine, if_exists="replace")

# %%
