"""
This python module will obtain data from section elements
of the bootstrap-static/ endpoint of the premier league API.
This section contains elements information of all Premier League players
including points, status, value, match stats (goals, assists, etc.), ICT index, etc.

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

# create elements dataframe
elements_df = pd.DataFrame(data['elements'])

# Rearrange columns in elements_df
first_column = elements_df.pop('id')
second_column = elements_df.pop('first_name')
third_column = elements_df.pop('second_name')
fourth_column = elements_df.pop('web_name')
fifth_column = elements_df.pop('team')

elements_df.insert(0, 'id', first_column)
elements_df.insert(1, 'first_name', second_column)
elements_df.insert(2, 'second_name', third_column)
elements_df.insert(3, 'web_name', fourth_column)
elements_df.insert(4, 'team', fifth_column)

# Convert all columns with data type "string" in "float"
elements_df['ep_next'] = elements_df['ep_next'].astype('float')
elements_df['ep_this'] = elements_df['ep_this'].astype('float')
elements_df['form'] = elements_df['form'].astype('float')
elements_df['points_per_game'] = elements_df['points_per_game'].astype('float')
elements_df['selected_by_percent'] = elements_df['selected_by_percent'].astype('float')
elements_df['value_form'] = elements_df['value_form'].astype('float')
elements_df['value_season'] = elements_df['value_season'].astype('float')
elements_df['influence'] = elements_df['influence'].astype('float')
elements_df['creativity'] = elements_df['creativity'].astype('float')
elements_df['threat'] = elements_df['threat'].astype('float')
elements_df['ict_index'] = elements_df['ict_index'].astype('float')
elements_df['expected_goals'] = elements_df['expected_goals'].astype('float')
elements_df['expected_assists'] = elements_df['expected_assists'].astype('float')
elements_df['expected_goal_involvements'] = elements_df['expected_goal_involvements'].astype('float')
elements_df['expected_goals_conceded'] = elements_df['expected_goals_conceded'].astype('float')

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
    cur.execute("DROP TABLE IF EXISTS Elements;")
    print("Table deleted")

except psycopg2.Error as e:
    print(f'Error: {e}')

# Create Elements table
try:
    sql = "CREATE TABLE IF NOT EXISTS Elements (id VARCHAR(255), first_name VARCHAR(255),\
                second_name VARCHAR(255), web_name VARCHAR(255), team INT, chance_of_playing_next_round FLOAT,\
                chance_of_playing_this_round FLOAT, code INT, cost_change_event INT, cost_change_event_fall INT,\
                cost_change_start INT, cost_change_start_fall INT, dreamteam_count INT, element_type INT,\
                ep_next FLOAT, ep_this FLOAT, event_points INT, form FLOAT, in_dreamteam VARCHAR(255), \
                news VARCHAR(255), news_added VARCHAR(255),\
                now_cost INT, photo VARCHAR(255), points_per_game FLOAT, selected_by_percent FLOAT,\
                special VARCHAR(255),squad_number VARCHAR(255), status VARCHAR(255), team_code INT, total_points INT,\
                transfers_in INT, transfers_in_event INT, transfers_out INT, transfers_out_event INT, value_form FLOAT,\
                 value_season FLOAT,\
                minutes INT, goals_scored INT, assists INT, clean_sheets INT, goals_conceded INT, own_goals INT,\
                penalties_saved INT, penalties_missed INT, yellow_cards INT, red_cards INT, saves INT, bonus INT,\
                bps INT, influence FLOAT, creativity FLOAT, threat FLOAT, ict_index FLOAT, starts INT,\
                expected_goals FLOAT, expected_assists FLOAT, expected_goal_involvements FLOAT,\
                expected_goals_conceded FLOAT, \
                influence_rank INT, influence_rank_type INT, creativity_rank INT, creativity_rank_type INT,\
                threat_rank INT, threat_rank_type INT, ict_index_rank INT, ict_index_rank_type INT,\
                corners_and_indirect_freekicks_order FLOAT, corners_and_indirect_freekicks_text VARCHAR(255),\
                direct_freekicks_order FLOAT, direct_freekicks_text VARCHAR(255), penalties_order FLOAT,\
                penalties_text VARCHAR(255), expected_goals_per_90 FLOAT, saves_per_90 FLOAT,\
                expected_assists_per_90 FLOAT,expected_goal_involvements_per_90 FLOAT,\
                expected_goals_conceded_per_90 FLOAT, goals_conceded_per_90 FLOAT,\
                now_cost_rank INT, now_cost_rank_type INT, form_rank INT, form_rank_type INT, points_per_game_rank INT,\
                points_per_game_rank_type INT, selected_rank INT, selected_rank_type INT, starts_per_90 FLOAT,\
                clean_sheets_per_90 FLOAT);"

    cur.execute(sql)
    print("Table created")

except psycopg2.Error as e:
    print(f'Error: {e}')

# Load data

execute_values(conn, elements_df, 'Elements')

cur.close()
conn.close()
