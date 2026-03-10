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
url = "https://pinnacle-odds.p.rapidapi.com/kit/v1/markets"
params = {"event_type":"prematch","sport_id":"1","is_have_odds":"true"}
headers = {
    "x-rapidapi-key": RAPIDAPI_KEY,
    "x-rapidapi-host": "pinnacle-odds.p.rapidapi.com"
}


def get_events(url, headers, params):
    """
    Fetch the events using the API 

    """
    try:
        events = requests.get(url, headers=headers, params=params)
        events.raise_for_status()
        return events.json()

    except requests.exceptions.HTTPError as http_error_message:
        print (f"❌ [HTTP ERROR]: {http_error_message}")

    except requests.exceptions.ConnectionError as connection_error_message:
        print (f"❌ [CONNECTION ERROR]: {connection_error_message}")

    except requests.exceptions.Timeout as timeout_error_message:
        print (f"❌ [TIMEOUT ERROR]: {timeout_error_message}")

    except requests.exceptions.RequestException as other_error_message:
        print (f"❌ [UNKNOWN ERROR]: {other_error_message}")

def process_events(data):
    """
    Parse the JSON data required for events.
    """
    events = []
    
    if 'events' not in data:
        print("❌ Error: 'response' key not found in API response.")
        return []

    for events_data in data['events']:  # Assuming API structure uses 'events' key
        # Extract values
        liiga_id = events_data['league_id']
        liiga_nimi = events_data['league_name']
        ottelu_id = events_data['event_id']
        alkaa_ajankohta = events_data['starts']
        kotijoukkue = events_data['home']
        vierasjoukkue = events_data['away']

        # Validate necessary fields
        if kotijoukkue is None or vierasjoukkue is None:
            print(f"⚠️ Missing data for league: {events_data}")
            continue


    # Handle errors in kerroin_koti, kerroin_tasan, and kerroin_vieras
        try:
            kerroin_koti = float(events_data['periods']['num_0']['money_line']['home'])
        except (KeyError, ValueError, TypeError):
            kerroin_koti = 0

        try:
            kerroin_tasan = float(events_data['periods']['num_0']['money_line']['draw'])
        except (KeyError, ValueError, TypeError):
            kerroin_tasan = 0

        try:
            kerroin_vieras = float(events_data['periods']['num_0']['money_line']['away'])
        except (KeyError, ValueError, TypeError):
            kerroin_vieras = 0


        # Append data
        events.append({
            'league_id':liiga_id,
            'league_name':liiga_nimi,
            'events_id': ottelu_id,
            'starts':alkaa_ajankohta,
            'home': kotijoukkue,
            'away': vierasjoukkue,
            'kerroin_koti': kerroin_koti,
            'kerroin_tasan': kerroin_tasan,
            'kerroin_vieras': kerroin_vieras
        })

    return events


def create_dataframe(events):
    """
    Convert list of dictionaries into a Pandas dataframe and process it
    """

    df = pd.DataFrame(events)

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
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE_PINNACLE_ODDS')
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
    CREATE TABLE IF NOT EXISTS pinnacle_kertoimet (
        `liiga_id` VARCHAR(255),
        `liiga_nimi` VARCHAR(255),
        `ottelu_id` VARCHAR(255),
        `alkaa_ajankohta` VARCHAR(255),
        `kotijoukkue` VARCHAR(255),
        `vierasjoukkue` VARCHAR(255),
        `kerroin_koti` FLOAT,
        `kerroin_tasan` FLOAT,
        `kerroin_vieras` FLOAT,
        PRIMARY KEY (`ottelu_id`)
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
    cursor.execute("TRUNCATE TABLE pinnacle_kertoimet;")
    db_connection.commit()

    # Define the INSERT SQL query
    INSERT_DATA_SQL_QUERY = """
    INSERT INTO pinnacle_kertoimet (
    `liiga_id`, `liiga_nimi`, `ottelu_id`, `alkaa_ajankohta`, `kotijoukkue`, `vierasjoukkue`, `kerroin_koti`, `kerroin_tasan`,`kerroin_vieras`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        `liiga_id` = VALUES(`liiga_id`),
        `liiga_nimi` = VALUES(`liiga_nimi`),
        `ottelu_id` = VALUES(`ottelu_id`),
        `alkaa_ajankohta` = VALUES(`alkaa_ajankohta`),
        `kotijoukkue` = VALUES(`kotijoukkue`),
        `vierasjoukkue` = VALUES(`vierasjoukkue`),
        `kerroin_koti` = VALUES(`kerroin_koti`),
        `kerroin_tasan` = VALUES(`kerroin_tasan`),
        `kerroin_vieras` = VALUES(`kerroin_vieras`)
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

    data = get_events(url, headers, params)

    if data and 'events' in data and data['events']:
        events = process_events(data)
        df = create_dataframe(events)
        print(df.to_string(index=False)) 

    else:
        print("No data available or an error occurred ❌")

    db_connection = create_db_connection(HOST, MYSQL_USERNAME, MYSQL_PASSWORD, MYSQL_DATABASE)


    # If connection is successful, proceed with creating table and inserting data
    if db_connection is not None:
        create_table(db_connection)  
        df = create_dataframe(events) 
        insert_into_table(db_connection, df)  

if __name__ == "__main__":
    run_data_pipeline()
