import pyodbc 
cnxn = pyodbc.connect('DRIVER={Devart ODBC Driver for SQLAzure};Server=animalserver.database.windows.net;Database=animaldatabase;Port=1433;User ID=azureuser;Password=Azure_Password')