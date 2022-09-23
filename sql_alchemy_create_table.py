
import pyodbc
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData
import urllib
import json
import requests
import pandas as pd

server = 'animalserver.database.windows.net'
database = 'animaldatabase'
username = 'azureuser'
password = 'Azure_Password' 
driver= '{ODBC Driver 18 for SQL Server}'

#params = urllib.parse.quote_plus \
#(r'Driver={ODBC Driver 13 for SQL Server};Server=tcp:yourDBServerName.database.windows.net,1433;Database=dbname;Uid=username@dbserverName;Pwd=xxx;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')

params = urllib.parse.quote_plus \
('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
engine_azure = create_engine(conn_str,echo=True)

print('connection is ok')

# Create the Metadata Object
meta = MetaData()
animal_name_shelter = Table(
    'animal_name_shelter', meta,
    Column('animal_id', String(20), primary_key = True),
    Column('name', String),
    Column('animal_type', String),
    Column('breed', String),
    Column('color', String),
)
meta.create_all(engine_azure)


animal_shelter_api = requests.get("https://data.austintexas.gov/resource/9t4d-g238.json?$limit=50")
animal_shelter_table = animal_shelter_api.text
json.loads(animal_shelter_table)
animal_shelter_table = pd.read_json(animal_shelter_table)


name = animal_shelter_table[["animal_id","name","animal_type","breed","color"]]
#print(name.head())
name.reset_index(drop=True, inplace=True)


    
name.to_sql('animal_name_shelter', con=engine_azure, if_exists='replace', index=False)