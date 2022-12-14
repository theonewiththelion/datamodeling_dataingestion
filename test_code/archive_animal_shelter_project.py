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


#Create tables:

#Extract

    #Extract information from API
    #Save API into variable animal_shelter_table
    #Convert variable austin_crime into JSON format
    #Save Json file into a dataframe
    #Store the information into 3 tables: name, dates & outcome
    #Create table namne
    #Create table dates
    #Create table outcome
    #Select columns from API DF to specific dataframe
    #Use dataframes to create the data of the tables 
    
    
def api_extraction():
    
    #animal_shelter_api = requests.get("https://data.austintexas.gov/resource/9t4d-g238.csv?$limit=10")
    animal_shelter_api = requests.get("https://data.austintexas.gov/resource/9t4d-g238.json?$limit=1000")
    animal_shelter_table = animal_shelter_api.text
    json.loads(animal_shelter_table)
    animal_shelter_table = pd.read_json(animal_shelter_table)
    #print(animal_shelter_table)
    print(animal_shelter_table.columns)
     
    names = Table(
        'names', meta,
        Column('animal_id', String(20), primary_key = True),
        Column('name', String),
        Column('animal_type', String),
        Column('breed', String),
        Column('color', String),
    )
    
    dates = Table(
        'dates', meta,
        Column('animal_id', String(20), primary_key = True),
        Column('datetime', DateTime),
        Column('monthyear', String),
        Column('date_of_birth', String)
    )
    
    outcome = Table(
        'outcome', meta,
        Column('animal_id', String(20), primary_key = True),
        Column('outcome_type', String),
        Column('sex_upon_outcome', String),
        Column('age_upon_outcome', String)
    )
    
    meta.create_all(engine_azure)
    print("Tables created")
    print(animal_shelter_table.columns)
    
    names = animal_shelter_table[["animal_id","name","animal_type","breed","color"]]
    print(names.head())
    
    dates = animal_shelter_table[["animal_id","datetime","monthyear","date_of_birth"]]
    print(dates.head())
    
    outcome = animal_shelter_table[["animal_id","outcome_type","sex_upon_outcome","age_upon_outcome"]]
    print(outcome.head())

    #print(name.head())
    #name.reset_index(drop=True, inplace=True)

    names.to_sql('names', con=engine_azure, if_exists='replace', index=False)
    print("Table names Populated")
    
    dates.to_sql('dates', con=engine_azure, if_exists='replace', index=False)
    print("Table dates Populated")
    
    outcome.to_sql('outcome', con=engine_azure, if_exists='replace', index=False)
    print("Table outcome Populated")
    
    
api_extraction()

    #Transform
    #Extract data from the tables; Names, dates & Outcome
    
    #Transform dates
    #For date time, just select the time, remove date
    #for month year, select the month and the year, remove time
    #date of bith, remove the time (after value T)
    
    #Get data from tables, transform it and upload back
    # SQLAlchemy connectable

def transformation():
  
#Documentation https://learn.microsoft.com/en-us/sql/machine-learning/data-exploration/python-dataframe-pandas?view=sql-server-ver16

    query_dates = "SELECT * FROM dates"
    extract_dates = pd.read_sql(query_dates, conn_str)
    print(extract_dates)
    
    #documentation - https://datascienceparichay.com/article/pandas-split-column-by-delimiter/
    #take month year and separrate the values
    extract_dates[['Date', 'date_time']] = extract_dates['monthyear'].str.split('T', expand = True)
    
    extract_dates[['date_year', 'date_month', 'date_day']] = extract_dates['Date'].str.split('-', expand = True)
    
    #Documentation
    #https://stackoverflow.com/questions/40705480/python-pandas-remove-everything-after-a-delimiter-in-a-string
    #Separate the field date of birth after the value "T"
    extract_dates['date_of_birth'] = extract_dates['date_of_birth'].str.split('T').str[0]
    
    #extract_dates['date_time'] = extract_dates['date_time'].str.split('T', n=1).str[0]
     
     
     
    #drop "datetime", "monthyear"
    #extract_dates.drop(['datetime','monthyear'], axis=1)
    del extract_dates['datetime']
    del extract_dates['monthyear']
    
    print(extract_dates)
    
    
    extract_dates.to_sql('dates', con=engine_azure, if_exists='replace', index=False)
    print("Table dates transformed and updated")
    
    
#date time, remove month year, and split date time in two columns one for date, other for time and find what they mean lol

transformation()

#Create 3 new "Production" tables
def load_animal_shelter():

#Load new tables into "Production"
#Store production values in it
#Documentation https://learn.microsoft.com/en-us/sql/machine-learning/data-exploration/python-dataframe-pandas?view=sql-server-ver16

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
    
    
    query_names = "SELECT * FROM names"
    extract_names = pd.read_sql(query_names, conn_str)
    print(extract_names)
    
    query_dates = "SELECT * FROM dates"
    extract_dates = pd.read_sql(query_dates, conn_str)
    print(extract_dates)
    
    query_outcome = "SELECT * FROM outcome"
    extract_outcome = pd.read_sql(query_outcome, conn_str)
    print(extract_outcome)
    
    
    
    extract_names.to_sql('p_names', con=engine_azure, if_exists='replace', index=False)
    print("Table names Populated")
    
    extract_dates.to_sql('p_dates', con=engine_azure, if_exists='replace', index=False)
    print("Table dates Populated")
    
    extract_outcome.to_sql('p_outcome', con=engine_azure, if_exists='replace', index=False)
    print("Table outcome Populated")
    
load_animal_shelter()