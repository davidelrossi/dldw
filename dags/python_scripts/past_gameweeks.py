"""
This python module will obtain data from section history
of the element-summary/{player_id}/ endpoint of the premier league API.
This section contains a list of player’s previous fixtures and its match stats.

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

# Create a list of all players' ids
elements = pd.DataFrame(data['elements'])
ids_list = elements['id'].tolist()

# Get all gameweek info for all players

# Empty dataframe
gameweeks_df = pd.DataFrame()

# Progress counter
counter = 0

# Loop through the urls
for i in ids_list:
    url_details = f'https://fantasy.premierleague.com/api/element-summary/{i}/'
    r = requests.get(url_details).json()

    df = pd.json_normalize(r['history'])

    gameweeks_df = pd.concat([gameweeks_df, df])

    counter += 1
    if (counter % 50) == 0 or counter == len(ids_list):
        print(str(counter) + " players")

# change data types
gameweeks_df['influence'] = gameweeks_df['influence'].astype('float')
gameweeks_df['creativity'] = gameweeks_df['creativity'].astype('float')
gameweeks_df['threat'] = gameweeks_df['threat'].astype('float')
gameweeks_df['ict_index'] = gameweeks_df['ict_index'].astype('float')
gameweeks_df['expected_goals'] = gameweeks_df['expected_goals'].astype('float')
gameweeks_df['expected_assists'] = gameweeks_df['expected_assists'].astype('float')
gameweeks_df['expected_goal_involvements'] = gameweeks_df['expected_goal_involvements'].astype('float')
gameweeks_df['expected_goals_conceded'] = gameweeks_df['expected_goals_conceded'].astype('float')

# Fill NaN with 0. NaNs occur during a gameday when the matches have not yet been played.
gameweeks_df['team_h_score'] = gameweeks_df['team_h_score'].fillna(0)
gameweeks_df['team_a_score'] = gameweeks_df['team_a_score'].fillna(0)

# Connect to Data Lake
host = "datalake1.clfypptwx2in.us-east-1.rds.amazonaws.com"
database = "fpl_api"
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
    cur.execute("DROP TABLE IF EXISTS Gameweeks;")
    print("Table deleted")

except psycopg2.Error as e:
    print(f'Error: {e}')

# Create Teams table
try:
    sql = "CREATE TABLE IF NOT EXISTS Gameweeks (element INT, fixture INT, opponent_team INT, total_points INT,\
    was_home VARCHAR(255), kickoff_time VARCHAR(255), team_h_score INT, team_a_score INT, round INT, minutes INT,\
    goals_scored INT, assists INT, clean_sheets INT, goals_conceded INT, own_goals INT, penalties_saved INT,\
    penalties_missed INT, yellow_cards INT, red_cards INT, saves INT, bonus INT, bps INT, influence FLOAT,\
    creativity FLOAT, threat FLOAT, ict_index FLOAT, starts INT, expected_goals FLOAT, expected_assists FLOAT,\
    expected_goal_involvements FLOAT, expected_goals_conceded FLOAT, value INT, transfers_balance INT, selected INT,\
    transfers_in INT, transfers_out INT);"
    cur.execute(sql)
    print("Table created")

except psycopg2.Error as e:
    print(f'Error: {e}')

# Load data

execute_values(conn, gameweeks_df, 'Gameweeks')

cur.close()
conn.close()


