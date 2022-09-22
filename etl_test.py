import pandas as pd
import pyodbc

server = 'animalserver.database.windows.net'
database = 'animaldatabase'
username = 'azureuser'
password = 'Azure_Password' 
driver= '{ODBC Driver 18 for SQL Server}'

with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
#with pyodbc.connect('ODBC Driver 18 for SQL Server','animalserver.database.windows.net','PORT=1433','animaldatabase','azureuser','Azure_Password') as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT @@Version")
        row = cursor.fetchall()
        print(row)




