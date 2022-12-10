"""
This python module will obtain data from section history_past
of the element-summary/{player_id}/ endpoint of the premier league API.
This section contains a list of player’s previous seasons and its seasonal stats.

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

# Get all season history info for all players

# Empty dataframe
past_seasons_df = pd.DataFrame()

# Progress counter
counter = 0

for i in ids_list:
    url_details = f'https://fantasy.premierleague.com/api/element-summary/{i}/'
    r = requests.get(url_details).json()

    df = pd.json_normalize(r['history_past'])

    past_seasons_df = pd.concat([past_seasons_df, df])

    counter += 1
    if (counter % 50) == 0 or counter == len(ids_list):
        print(str(counter) + " players")

# change data types
past_seasons_df['influence'] = past_seasons_df['influence'].astype('float')
past_seasons_df['creativity'] = past_seasons_df['creativity'].astype('float')
past_seasons_df['threat'] = past_seasons_df['threat'].astype('float')
past_seasons_df['ict_index'] = past_seasons_df['ict_index'].astype('float')
past_seasons_df['expected_goals'] = past_seasons_df['expected_goals'].astype('float')
past_seasons_df['expected_assists'] = past_seasons_df['expected_assists'].astype('float')
past_seasons_df['expected_goal_involvements'] = past_seasons_df['expected_goal_involvements'].astype('float')
past_seasons_df['expected_goals_conceded'] = past_seasons_df['expected_goals_conceded'].astype('float')

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
    cur.execute("DROP TABLE IF EXISTS Past_seasons;")
    print("Table deleted")

except psycopg2.Error as e:
    print(f'Error: {e}')

# Create Teams table
try:
    sql = "CREATE TABLE IF NOT EXISTS Past_seasons (season_name VARCHAR(255), element_code INT, start_cost INT,\
            end_cost INT, total_points INT, minutes INT, goals_scored INT, assists INT, clean_sheets INT,\
            goals_conceded INT, own_goals INT, penalties_saved INT, penalties_missed INT, yellow_cards INT,\
            red_cards INT, saves INT, bonus INT, bps INT, influence FLOAT, creativity FLOAT, threat FLOAT,\
            ict_index FLOAT, starts INT, expected_goals FLOAT,expected_assists FLOAT, expected_goal_involvements FLOAT,\
            expected_goals_conceded FLOAT);"
    cur.execute(sql)
    print("Table created")

except psycopg2.Error as e:
    print(f'Error: {e}')

# Load data

execute_values(conn, past_seasons_df, 'Past_seasons')

cur.close()
conn.close()
