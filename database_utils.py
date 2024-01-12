#%%
import yaml    
import sqlalchemy
from sqlalchemy import inspect
from sqlalchemy import text
import psycopg2
import pandas as pd


#%%

class DatabaseConnector:

    def __init__(self):
        pass

        
    def read_db_creds(self):
        with open("db_creds.yaml", "r") as self.db_creds:
            self.db_creds = yaml.safe_load(self.db_creds)
        self.db_creds = list(self.db_creds.values())
        

    def init_db_engine(self):
        self.read_db_creds()
        HOST, PASSWORD, USER, DATABASE, PORT = [self.db_creds[i] for i in (0, 1, 2, 3, 4)] 
        source_engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return source_engine


    def list_db_tables(self):
        source_engine = self.init_db_engine()
        inspector = inspect(source_engine)
        print(inspector.get_table_names())

'''
    def upload_to_db(self, df, table_name):
        
        HOST, PASSWORD, USER, DATABASE, PORT = [self.db_creds[i] for i in (5, 6, 7, 8, 9)] 
        sink_engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        df.to_sql("table_name", sink_engine, if_exists="replace")

        '''
        
if __name__ == "__main__":
    dbsconnector = DatabaseConnector()
    dbsconnector.read_db_creds()
    #print(dbsconnector.db_creds)
    #dbsconnector.init_db_engine()
    #dbsconnector.list_db_tables()
    

# %%
'''
def upload_user_data():
    

    DatabaseConnector.upload_to_db(user_data, dim_users)



if __name__ == "__main__":
    upload_user_data()
    
'''
# %%
