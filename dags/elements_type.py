"""
This python module will obtain data from section elements_type
of the bootstrap-static/ endpoint of the premier league API.
This section contains basic information about player’s position (GK, DEF, MID, FWD)

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
elements_types_df = pd.DataFrame(data['element_types'])

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
    cur.execute("DROP TABLE IF EXISTS Elements_types;")
    print("Table deleted")

except psycopg2.Error as e:
    print(f'Error: {e}')

# Create Elements_types table
try:
    sql = "CREATE TABLE IF NOT EXISTS Elements_types (id INT, plural_name VARCHAR(255), plural_name_short VARCHAR(255),\
            singular_name VARCHAR(255), singular_name_short VARCHAR(255), squad_select INT, squad_min_play INT,\
            squad_max_play INT, ui_shirt_specific VARCHAR(255), sub_positions_locked VARCHAR(255), element_count INT);"
    cur.execute(sql)
    print("Table created")

except psycopg2.Error as e:
    print(f'Error: {e}')

# Load data

execute_values(conn, elements_types_df, 'Elements_types')

cur.close()
conn.close()