"""
Load a list of all player's IDs
"""

import requests
import pandas as pd
# import psycopg2
# from functions.execute_values import execute_values

# API url
url = 'https://fantasy.premierleague.com/api/bootstrap-static/'

# Get data from url
response = requests.get(url)

# Convert the data in json format
data = response.json()

elements = pd.DataFrame(data['elements'])
ids_list = elements['id'].tolist()
print(len(ids_list))
