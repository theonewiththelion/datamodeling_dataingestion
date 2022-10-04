from datetime import date, datetime
import pyodbc
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, DateTime, Float
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



#Extraction
    
#Extract data from API
#Save information into dataframe called names    
def api_extraction_names():
    
    #animal_shelter_api = requests.get("https://data.austintexas.gov/resource/9t4d-g238.csv?$limit=10")
    animal_shelter_api = requests.get("https://data.austintexas.gov/resource/9t4d-g238.json?$limit=10")
    animal_shelter_table = animal_shelter_api.text
    json.loads(animal_shelter_table)
    animal_shelter_table = pd.read_json(animal_shelter_table)

    
    names = animal_shelter_table[["animal_id","name","animal_type","breed","color"]]
    print(names.head())

    return names

#Extract data from API
#Save information into dataframe called dates    
def api_extraction_dates():

    #animal_shelter_api = requests.get("https://data.austintexas.gov/resource/9t4d-g238.csv?$limit=10")
    animal_shelter_api = requests.get("https://data.austintexas.gov/resource/9t4d-g238.json?$limit=10")
    animal_shelter_table = animal_shelter_api.text
    json.loads(animal_shelter_table)
    animal_shelter_table = pd.read_json(animal_shelter_table)

    
    dates = animal_shelter_table[["animal_id","datetime","monthyear","date_of_birth"]]
    print(dates.head())
    
    return dates

#Extract data from API
#Save information into dataframe called outcome    
def api_extraction_outcome():
    #animal_shelter_api = requests.get("https://data.austintexas.gov/resource/9t4d-g238.csv?$limit=10")
    animal_shelter_api = requests.get("https://data.austintexas.gov/resource/9t4d-g238.json?$limit=10")
    animal_shelter_table = animal_shelter_api.text
    json.loads(animal_shelter_table)
    animal_shelter_table = pd.read_json(animal_shelter_table)
    
    outcome = animal_shelter_table[["animal_id","outcome_type","sex_upon_outcome","age_upon_outcome"]]
    print(outcome.head())
    
    return outcome


#Transformation
#Separate values from column monthyear into two new columns date and date time, information from dates dataframe (not yet a table)
#Separate the column date, into 3 new columns date year, date motnh, date day
#separate the colum date of birth and accepting only everything before the value "T"
#Delete the columns datetime and monthyear
def transformation_dates(dates):
  
    #Documentation https://learn.microsoft.com/en-us/sql/machine-learning/data-exploration/python-dataframe-pandas?view=sql-server-ver16

    #documentation - https://datascienceparichay.com/article/pandas-split-column-by-delimiter/
    #take month year and separrate the values
    dates[['Date', 'date_time']] = dates['monthyear'].str.split('T', expand = True)
    
    dates[['date_year', 'date_month', 'date_day']] = dates['Date'].str.split('-', expand = True)
    
    #Documentation
    #https://stackoverflow.com/questions/40705480/python-pandas-remove-everything-after-a-delimiter-in-a-string
    #Separate the field date of birth after the value "T"
    dates['date_of_birth'] = dates['date_of_birth'].str.split('T').str[0]
    
    #drop "datetime", "monthyear"
    #extract_dates.drop(['datetime','monthyear'], axis=1)
    del dates['datetime']
    del dates['monthyear']
    
    print(dates)
    return dates

#Load
#Create tables p_name, p_dates & p_outcome
#Add the dataframe into the tables
#Call functions
def load_animal_shelter(names, outcome, dates):
    
    p_names = Table(
        'p_names', meta,
        Column('animal_id', String(20), primary_key = True),
        Column('name', String),
        Column('animal_type', String),
        Column('breed', String),
        Column('color', String),
    )
    
    p_dates = Table(
        'p_dates', meta,
        Column('animal_id', String(20), primary_key = True),
        Column('datetime', DateTime),
        Column('monthyear', String),
        Column('date_of_birth', String)
    )
    
    p_outcome = Table(
        'p_outcome', meta,
        Column('animal_id', String(20), primary_key = True),
        Column('outcome_type', String),
        Column('sex_upon_outcome', String),
        Column('age_upon_outcome', String)
    )
    
    meta.create_all(engine_azure)
    print("Tables created")
    
    
    names.to_sql('p_names', con=engine_azure, if_exists='replace', index=False)
    print("Table names Populated")
    
    outcome.to_sql('p_dates', con=engine_azure, if_exists='replace', index=False)
    print("Table dates Populated")
    
    dates.to_sql('p_outcome', con=engine_azure, if_exists='replace', index=False)
    print("Table outcome Populated")

if __name__ == "__main__":
    #Extraction
    names_df = api_extraction_names()
    dates_df = api_extraction_dates()
    outcome_df = api_extraction_outcome()
    
    #Transformation
    #The argument is the variable you assigned the function to
    dates_transformation_df = transformation_dates(dates_df)
    
    #Load
    #The argument is the variable you assigned the function to
    data_load = load_animal_shelter(names_df, dates_df, dates_transformation_df)
    
    
    print("DONE")