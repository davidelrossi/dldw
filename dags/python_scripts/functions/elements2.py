"""
This python module will obtain data from section elements
of the bootstrap-static/ endpoint of the premier league API.
This section contains elements information of all Premier League players
including points, status, value, match stats (goals, assists, etc.), ICT index, etc.

"""
import requests
import pandas as pd

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


elements_df.to_csv('elements.csv', encoding='utf-8-sig')