"""
This python module will obtain data from section fixtures
of the element-summary/{player_id}/ endpoint of the premier league API.
This section contains a list of player’s remaining fixtures of the season.

"""
import requests
import pandas as pd
import psycopg2
from functions.execute_values import execute_values

# API url
url = 'https://fantasy.premierleague.com/api/bootstrap-static/'

# Get data from url
response = requests.get(url)

# Convert the data in json format
data = response.json()

# Create list of all players' ids
elements = pd.DataFrame(data['elements'])
ids_list = elements['id'].tolist()

# Get all fixtures info for all players

# Empty dataframe
fixtures_df = pd.DataFrame()

# Progress counter
counter = 0

# Loop through the urls
for i in ids_list:
    url_details = f'https://fantasy.premierleague.com/api/element-summary/{i}/'
    r = requests.get(url_details).json()

    df = pd.json_normalize(r['fixtures'])

    fixtures_df = pd.concat([fixtures_df, df])

    counter += 1
    if (counter % 50) == 0 or counter == len(ids_list):
        print(str(counter) + " players")

# Connect to Data Lake
host = "datalake1.clfypptwx2in.us-east-1.rds.amazonaws.com"
database = "datalake1"
user = "danny"
password = "1234567890"

conn = psycopg2.connect(host=host,
                        database=database,
                        user=user,
                        password=password)
try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error: Could not get curser to the Database")
    print(e)

# Auto commit is very important
conn.set_session(autocommit=True)

# Drop existing table
try:
    cur.execute("DROP TABLE IF EXISTS Fixtures;")
    print("Table deleted")

except psycopg2.Error as e:
    print(f'Error: {e}')

# Create Teams table
try:
    sql = "CREATE TABLE IF NOT EXISTS Fixtures (id INT, code INT, team_h INT, team_h_score VARCHAR(255),\
    team_a INT, team_a_score VARCHAR(255), event FLOAT, finished VARCHAR(255), minutes INT,\
    provisional_start_time VARCHAR(255), kickoff_time VARCHAR(255), event_name VARCHAR(255), is_home VARCHAR(255),\
    difficulty INT);"
    cur.execute(sql)
    print("Table created")

except psycopg2.Error as e:
    print(f'Error: {e}')

# Load data

execute_values(conn, fixtures_df, 'Fixtures')

cur.close()
conn.close()
