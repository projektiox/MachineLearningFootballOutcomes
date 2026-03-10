import os
import requests
import pandas as pd
from datetime import datetime
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Load API key to make API requests
RAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY')

# Set up the API request details
url = "https://pinnacle-odds.p.rapidapi.com/kit/v1/leagues"
params = {"sport_id":"1"}
headers = {
    "X-RapidAPI-Key": RAPIDAPI_KEY,
    "X-RapidAPI-Host": "pinnacle-odds.p.rapidapi.com"
}


def get_leagues(url, headers, params):
    """
    Fetch the top scorers using the API 

    """
    try:
        leagues = requests.get(url, headers=headers, params=params)
        leagues.raise_for_status()
        return leagues.json()

    except requests.exceptions.HTTPError as http_error_message:
        print (f"❌ [HTTP ERROR]: {http_error_message}")

    except requests.exceptions.ConnectionError as connection_error_message:
        print (f"❌ [CONNECTION ERROR]: {connection_error_message}")

    except requests.exceptions.Timeout as timeout_error_message:
        print (f"❌ [TIMEOUT ERROR]: {timeout_error_message}")

    except requests.exceptions.RequestException as other_error_message:
        print (f"❌ [UNKNOWN ERROR]: {other_error_message}")

def process_leagues(data):
    """
    Parse the JSON data required for leagues.
    """
    leagues = []
    
    if 'leagues' not in data:
        print("❌ Error: 'response' key not found in API response.")
        return []

    for league_data in data['leagues']:  # Assuming API structure uses 'response' key
        # Extract values
        league_id = league_data.get('id')  # Use .get() to avoid KeyErrors
        league_name = league_data.get('name')
        home_team_type = league_data.get('homeTeamType')

        # Validate necessary fields
        if league_id is None or league_name is None or home_team_type is None:
            print(f"⚠️ Missing data for league: {league_data}")
            continue

        # Append data
        leagues.append({
            'id': league_id,
            'name': league_name,
            'home_team_type': home_team_type
        })

    return leagues


def create_dataframe(leagues):
    """
    Convert list of dictionaries into a Pandas dataframe and process it
    """

    df = pd.DataFrame(leagues)

    # Sort dataframe first by 'total_goals' in descending order, then by 'assists' in descending order
    # df.sort_values(by=['total_goals', 'assists'], ascending=[False, False], inplace=True)

    # Reset index after sorting to reflect new order
    # df.reset_index(drop=True, inplace=True)

    # Recalculate ranks based on the sorted order
    # df['position'] = df['total_goals'].rank(method='dense', ascending=False).astype(int)

    # Specify the columns to include in the final dataframe in the desired order
    # df = df[['position', 'player', 'club', 'total_goals', 'penalty_goals', 'assists', 'matches', 'mins', 'age']]

    return df

HOST = os.getenv('HOST')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE_PINNACLE_LEAGUES')
MYSQL_USERNAME = os.getenv('MYSQL_USERNAME')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')

def create_db_connection(host_name, user_name, user_password, db_name):
    """
    Establish a connection to the MySQL database
    """
    db_connection = None
    try:
        db_connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful ✅")

    except Error as e:
        print(f"❌ [DATABASE CONNECTION ERROR]: '{e}'")

    return db_connection

def create_table(db_connection):
    """
    Create a table if it does not exist in the MySQL database

    """

    CREATE_TABLE_SQL_QUERY = """
    CREATE TABLE IF NOT EXISTS pinnacle_leagues (
        `id` INT,
        `league_name` VARCHAR(255),
        `homeTeamType` VARCHAR(255),
        PRIMARY KEY (`id`)
    );
    """
    try:
        cursor = db_connection.cursor()
        cursor.execute(CREATE_TABLE_SQL_QUERY)
        db_connection.commit()
        print("Table created successfully ✅")

    except Error as e:
        print(f"❌ [CREATING TABLE ERROR]: '{e}'")

def insert_into_table(db_connection, df):
    """
    Insert or update the top scorers data in the database from the dataframe.
    First, the table is cleared, then the new data is inserted.
    """
    # Create a cursor from the db_connection
    cursor = db_connection.cursor()

    # Clear the table first
    cursor.execute("TRUNCATE TABLE pinnacle_leagues;")
    db_connection.commit()

    # Define the INSERT SQL query
    INSERT_DATA_SQL_QUERY = """
    INSERT INTO pinnacle_leagues (
        `id`, `league_name`, `homeTeamType`) VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE
        `league_name` = VALUES(`league_name`),
        `homeTeamType` = VALUES(`homeTeamType`)
    """

    # Create a list of tuples from the dataframe values
    data_values_as_tuples = [tuple(x) for x in df.to_numpy()]

    # Execute the INSERT query for all rows
    cursor.executemany(INSERT_DATA_SQL_QUERY, data_values_as_tuples)
    db_connection.commit()

    print("Data inserted or updated successfully ✅")

def run_data_pipeline():
    """
    Execute the ETL pipeline 
    """
    # check_rate_limits()

    data = get_leagues(url, headers, params)

    if data and 'leagues' in data and data['leagues']:
        leagues = process_leagues(data)
        df = create_dataframe(leagues)
        print(df.to_string(index=False)) 

    else:
        print("No data available or an error occurred ❌")

    db_connection = create_db_connection(HOST, MYSQL_USERNAME, MYSQL_PASSWORD, MYSQL_DATABASE)


    # If connection is successful, proceed with creating table and inserting data
    if db_connection is not None:
        create_table(db_connection)  
        df = create_dataframe(leagues) 
        insert_into_table(db_connection, df)  

if __name__ == "__main__":
    run_data_pipeline()
