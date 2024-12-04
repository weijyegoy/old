import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL 
from datetime import datetime
import time

SQL_SERVER = 'DBM_Public'
DATABASE = 'External'
USERNAME = 'dbm_db_owner'
PASSWORD = '!@#$qwer001'

connection_string = "DRIVER={SQL Server}"+f";SERVER={SQL_SERVER};UID={USERNAME};PWD={PASSWORD};DATABASE={DATABASE};"
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
engine = create_engine(connection_url)

Current_month = datetime.now().month - 1
if Current_month == 0:
    Current_month = 12
    Year = datetime.now().year - 1
else:
    Year = datetime.now().year

Month = f"{Year}0{Current_month}" if Current_month < 10 else f"{Year}{Current_month}"
Month = int(Month)

try:

    ATM_IN = pd.read_csv(f"{Month}/ATM_IN.csv",encoding = "BIG5")
    ATM_OUT = pd.read_csv(f"{Month}/ATM_OUT.csv", encoding="BIG5").query('機種 in ["ADM", "ATM"]')

    def upload_df(engine, table_name, df):
        start_time = time.time()
        df.to_sql(table_name, engine, if_exists='append', index=False)
        end_time = time.time()
        elapsed_time = end_time - start_time

        minutes = int(elapsed_time // 60)
        seconds = int(elapsed_time % 60)

        print(Month, 'ATM_ADM data was successfully uploaded to SQL server.')
        print(f"Time taken to upload: {minutes} minutes and {seconds} seconds")
        print("------------------------------------")
        
    def check_latest(engine, table_name, year):
        sql = f"SELECT max(distinct(統計時間)) FROM [External].[dbo].[{table_name}]"
        month = pd.read_sql(sql, engine)
        if int(month.values) == Month:
            result = True
        else:
            result = False
        return result

    if check_latest(engine,'M_永豐行內ATM_ADM', Month) != True:
        upload_df(engine, 'M_永豐行內ATM_ADM', ATM_IN)
    else:
        print(Month,"ATM_ADM data was already uploaded to SQL server.")

    if check_latest(engine,'M_永豐行外ATM_ADM', Month) != True:
        upload_df(engine, 'M_永豐行外ATM_ADM', ATM_OUT)
    else:
        print(Month,"ATM_ADM data was already uploaded to SQL server.")

except:
    print(Month,"data hasn't updated \nSkipping upload process")