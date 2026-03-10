import os
import requests
import json
from dotenv import load_dotenv

url = "https://pinnacle-odds.p.rapidapi.com/kit/v1/leagues"

params = {"sport_id": "1"}

# Load environment variables from .env file
load_dotenv()

# Load API key to make API requests
RAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY')

headers = {
    "X-RapidAPI-Key": RAPIDAPI_KEY,
    "x-rapidapi-host": "pinnacle-odds.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=params)

# Pretty print the JSON response
print(json.dumps(response.json(), indent=4, sort_keys=True))