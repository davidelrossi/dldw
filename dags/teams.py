"""
This python module will obtain data from section teams
of the bootstrap-static/ endpoint of the premier league API.
This section contains basic information of current Premier League clubs.

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

# Create DataFrame
teams_df = pd.DataFrame(data['teams'])

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
    cur.execute("DROP TABLE IF EXISTS Teams;")
    print("Table deleted")

except psycopg2.Error as e:
    print(f'Error: {e}')

# Create Teams table
try:
    sql = "CREATE TABLE IF NOT EXISTS Teams (code INT, draw INT, form VARCHAR(255),\
            id INT, loss INT, name VARCHAR(255),\
            played INT, points INT, position INT, short_name VARCHAR(255), strength INT, team_division VARCHAR(255),\
            unavailable VARCHAR(255), win INT, strength_overall_home INT, strength_overall_away INT,\
            strength_attack_home INT, strength_attack_away INT, strength_defence_home INT, strength_defence_away INT,\
            pulse_id INT);"
    cur.execute(sql)
    print("Table created")

except psycopg2.Error as e:
    print(f'Error: {e}')

# Load data

execute_values(conn, teams_df, 'Teams')

cur.close()
conn.close()
