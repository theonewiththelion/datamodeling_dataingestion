import pyodbc


server = 'animalserver.database.windows.net'
database = 'animaldatabase'
username = 'azureuser'
password = 'Azure_Password' 
driver= '{ODBC Driver 18 for SQL Server}'


cnxn = pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()
			
#get the count of tables with the name
#NEED TO MODIFY THE SCHEMA
#cursor.execute(''' SELECT count(name)  information_schema.tables WHERE tablename ='animal_name' ''')
cursor.execute(''' SELECT * FROM animaldatabase.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'animal_name' ''')

#if the count is 1, then table exists
if cursor.fetchone()[0]==1 : {
	print("Table exists.")
}
			
#commit the changes to db			
cnxn.commit()
#print("C")
#close the connection
cnxn.close()