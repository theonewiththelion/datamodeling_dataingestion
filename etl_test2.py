import pandas as pd
import pyodbc
import requests
import json


server = 'animalserver.database.windows.net'
database = 'animaldatabase'
username = 'azureuser'
password = 'Azure_Password' 
driver= '{ODBC Driver 18 for SQL Server}'


animal_shelter_api = requests.get("https://data.austintexas.gov/resource/9t4d-g238.json?$limit=10")
animal_shelter_table = animal_shelter_api.text
json.loads(animal_shelter_table)
animal_shelter_table = pd.read_json(animal_shelter_table)


name = animal_shelter_table[["animal_id","name","animal_type","breed","color"]]
print(name.head())



cnxn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

#create table in Azure
#If table exist
cursor.execute('''
		IF OBJECT_ID('animaldatabase.dbo.animal_name') IS NULL
			Begin
				CREATE TABLE animal_name 
					(
					animal_id nvarchar(50) primary key,
					name nvarchar(50),
					animal_type nvarchar(50),
					breed nvarchar(50),
					color nvarchar(50)
					)
 				END;
				''')
cnxn.commit()

#Add value to the table animal_name
# TEST if Table exist, then create record 8
# If not, create record 9

for index, row in name.iterrows():
     cursor.execute("INSERT INTO dbo.animal_name (animal_id,name,animal_type,breed,color) values(?,?,?,?,?)", row.animal_id, row.name, row.animal_type, row.breed, row.color)
cnxn.commit()
cursor.close()


# cnxn.execute('''
# 			INSERT INTO animal_name (animal_id, name, animal_type,breed,color)
# 			VALUES
# 				(12,'Piruli','Dog','Labrador','a')
#         ''')
# cnxn.commit()


