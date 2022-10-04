import pyodbc
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData
import urllib
import json
import requests
import pandas as pd

#Azure Server information
server = 'animalserver.database.windows.net'
database = 'animaldatabase'
username = 'azureuser'
password = 'Azure_Password' 
driver= '{ODBC Driver 18 for SQL Server}'

#Azure Server Information Parameters
params = urllib.parse.quote_plus('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
#Learn how to create the connection
conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
#Create Engine - VERY IMPORTANT
engine_azure = create_engine(conn_str,echo=True)
#Create metadata object, help at the creation of tables
meta = MetaData()


#Documentation https://learn.microsoft.com/en-us/sql/machine-learning/data-exploration/python-dataframe-pandas?view=sql-server-ver16
query_names = "SELECT * FROM names"
extract_names = pd.read_sql(query_names, conn_str)
print(extract_names)

query_dates = "SELECT * FROM dates"
extract_dates = pd.read_sql(query_dates, conn_str)
print(extract_dates)

query_outcome = "SELECT * FROM outcome"
extract_outcome = pd.read_sql(query_outcome, conn_str)
print(extract_outcome)