import pyodbc
from sqlalchemy import create_engine
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


animal_shelter_api = requests.get("https://data.austintexas.gov/resource/9t4d-g238.json?$limit=50")
animal_shelter_table = animal_shelter_api.text
json.loads(animal_shelter_table)
animal_shelter_table = pd.read_json(animal_shelter_table)


name = animal_shelter_table[["animal_id","name","animal_type","breed","color"]]
print(name.head())




name.to_sql('animal_name', con=engine_azure, if_exists='append')