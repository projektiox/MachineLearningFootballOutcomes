import os
import requests
import pandas as pd
from datetime import datetime
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import json  # Import json module for pretty printing

url = "https://api-football-v1.p.rapidapi.com/v3/players/topscorers"
querystring = {"league": "39", "season": "2024"}

headers = {
    "x-rapidapi-key": "b1b7e32aecmsh159daf694124df0p1706d3jsn4fe02bc48de4",
    "x-rapidapi-host": "api-football-v1.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

# Pretty print the JSON response
json_data = response.json()
print(json.dumps(json_data, indent=4))
