import requests

def get_teams_url():
    # API URL
    url = 'https://fantasy.premierleague.com/api/bootstrap-static/'

    response = requests.get(url)

    data = response.json()

    print(data)

get_teams_url()

