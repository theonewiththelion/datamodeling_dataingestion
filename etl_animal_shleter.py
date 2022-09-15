import pandas as pd
import pandas_gbq
import requests
import json

def api_extraction():

    animal_shelter_api = requests.get("https://data.austintexas.gov/resource/9t4d-g238.csv?$limit=10")
    #animal_shelter_api = requests.get("https://data.austintexas.gov/resource/9t4d-g238.json?$limit=10")
    #Save API into a variable
    animal_shelter_table = animal_shelter_api.text
    print(animal_shelter_table)


    """I want to see if I can see this comment on my mac
    """
api_extraction()
