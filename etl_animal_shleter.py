import pandas as pd

import requests
import json

def api_extraction():

    #animal_shelter_api = requests.get("https://data.austintexas.gov/resource/9t4d-g238.csv?$limit=10")
    animal_shelter_api = requests.get("https://data.austintexas.gov/resource/9t4d-g238.json?$limit=10")
    #Save API into a variable
    #print(animal_shelter_api)
    animal_shelter_table = animal_shelter_api.text
    #print(animal_shelter_table)

    # #Convert variable austin_crime into JSON format
    json.loads(animal_shelter_table)
    #print(animal_shelter_table)

    # #Save Json file into a dataframe
    animal_shelter_table = pd.read_json(animal_shelter_table)
    #print(animal_shelter_table)
    
   # print(animal_shelter_table.columns)

    name = animal_shelter_table[["animal_id","name","animal_type","breed","color"]]
    print(name.head())
    dates = animal_shelter_table[["animal_id","datetime","monthyear","date_of_birth"]]
    print(dates.head())
    outcome = animal_shelter_table[["animal_id","outcome_type","sex_upon_outcome","age_upon_outcome"]]
    #print(outcome)
    print(outcome.head())
    
    
api_extraction()
