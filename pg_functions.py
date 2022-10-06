import requests
import json
import datetime
import csv
import pandas as pd
import psycopg2 as pg
from sqlalchemy import create_engine
from strava_functions import obtain_api_access_info, get_past_runs, generate_runs_df

def generate_db_connection(path_to_creds, verbose=True):
    """
    This function connects to a postgres database, using the credentials specified in the path.
    Input: path to txt file with json credentials
    Output: a psycopg2 connection object
    """
    creds = obtain_api_access_info(path_to_creds)["postgres"]
    connection = pg.connect(user=creds["user"], password=creds["password"], host=creds["host"],port=creds["port"],database=creds["database"])
    if verbose:
        print("Connecting to {} database at {}:{}".format(creds["database"], creds["host"], creds["port"]))
    return connection
    
def get_last_logged_run(db_conn_obj, table_name, verbose=True):
    """
    This finds the most recent run logged in the database.
    Input: a pg database connection object
    Output: date-time string in the format '%Y-%m-%d %H:%M:%S' of the latest run logged in Postgres.
            This is the exact string format required as input for the get_past_runs() method.
    """
    cursor = db_conn_obj.cursor()
    search_query = "SELECT * FROM {} ORDER BY start_date DESC, start_time DESC LIMIT 1".format(table_name)
    cursor.execute(search_query)
    db_conn_obj.commit()
    query_result = cursor.fetchall()
    cursor.close()
    day = query_result[0][0] #datetime date object
    time = query_result[0][1] #datetime time object
    readable_datetime = day.strftime('%Y-%m-%d') + " " + time.strftime('%H:%M:%S')
    if verbose:
        print("Last run loaded to the database is:",readable_datetime)
    return readable_datetime

def write_to_postgres(db_conn_obj, table_name, df_to_add, verbose=True):
    """
    Writes tabular data from a dataframe into 
    Input: database connection object, table name, and a dataframe object to add
    Output: None
    """
    csv_rows = df_to_add.to_csv(None,index=False).split('\n')
    csv_rows = csv_rows[1:-1] #exclude the header row, and the empty row at the end
    insert_statement = "INSERT INTO {} (start_date, start_time, miles, hours, minutes) VALUES ".format(table_name)
    for line in csv_rows:
        line = line.rstrip('\r').split(',')
        insert_statement += "('{}', '{}', {:.1f}, {}, {}), ".format(line[0], line[1], float(line[2]), int(line[3]), int(line[4].rstrip()))
    insert_statement = insert_statement[:-2] + ";" #strip the extra comma and space; add a semicolon
    if verbose:
        if len(csv_rows) == 0:
            print("There is no API data available for the specified timeframe. Ending function execution.")
            return None
        else:
            print("{} rows written to {} table".format(len(csv_rows), table_name))
    cursor = db_conn_obj.cursor()
    cursor.execute(insert_statement)
    db_conn_obj.commit()
    cursor.close()
    return None
    
def update_postgres(path_to_creds, db_conn_obj, table_name, date_to_update_to, verbose=True, manual_start_date=None):
    '''
    Input: path to api & database credential file, connection object, table name, date to update to (%Y-%m-%d) 
    string path to csv file to which we'll append our data.
    ** If manual_start_date is NOT None, we'll pull and upload data from that date instead.
    This function reads the latest run logged in the database, then pulls all Strava runs from that last log date to the 
        date specified in the argument. It then writes these runs to the database.
    Output: None.
    '''
    date_to_update_to = date_to_update_to + " 23:59:59"
    if manual_start_date != None:
        strava_runs_output = get_past_runs(path_to_creds, manual_start_date + " 00:00:01", date_to_update_to, verbose=verbose)
    else:
        #we'll add one minute to the last logged date, to avoid duplicating entries
        last_logged_date = get_last_logged_run(db_conn_obj, table_name, verbose=verbose)
        last_db_date_datetime_obj = datetime.datetime.strptime(last_logged_date, '%Y-%m-%d %H:%M:%S')
        api_pull_start_datetime_obj = last_db_date_datetime_obj + datetime.timedelta(minutes=1)
        api_pull_start_datetime_string = api_pull_start_datetime_obj.strftime('%Y-%m-%d %H:%M:%S')
        #okay, let's actually get the runs with the fixed time now
        strava_runs_output = get_past_runs(path_to_creds, api_pull_start_datetime_string, date_to_update_to, verbose=verbose)
    df_of_runs = generate_runs_df(strava_runs_output, verbose=verbose)
    write_to_postgres(db_conn_obj, table_name, df_of_runs, verbose=verbose)
    return None

def display_db_as_df(db_conn_obj, table_name, start_date, end_date, verbose=True):
    """
    This function returns rows of the table within the time from start to end.
    Input: a psycopg2 connection object, table_name string, start date and end date in %Y-%m-%d (i.e. YYYY-MM-DD) format
    Output: a pandas dataframe. 
    """
    cursor = db_conn_obj.cursor()
    select_query = " \
    SELECT * FROM {} \
    WHERE start_date >= '{}' AND start_date <= '{}' \
    ORDER BY start_date DESC, start_time DESC;".format(table_name, start_date, end_date)
    cursor.execute(select_query)
    db_conn_obj.commit()
    result = cursor.fetchall()
    cursor.close()
    df = pd.DataFrame(result, columns=["start_date", "start_time", "miles", "hours", "minutes"])
    return df

def perform_sql_query(db_conn_obj, query_string, verbose=True):
    """
    This function performs an arbitrary SQL query into a specified table.
    Input: psycopg2 connection object, complete SQL query in string format
    Output: Pandas dataframe of the result
    """
    cursor = db_conn_obj.cursor()
    cursor.execute(query_string)
    db_conn_obj.commit()
    result = cursor.fetchall()
    cursor.close()
    df = pd.DataFrame(result, columns=["start_date", "start_time", "miles", "hours", "minutes"])
    if verbose:
        print(df)
    return df

def upload_from_csv(db_conn_obj, table_name, path_to_csv_input, verbose=True):
    """ TO-DO
    This function reads in a csv. CSV must be formatted as start_date, start_time, miles, hours, minutes (with header row)
    Input: psycopg2 connection object, table to upload to, path to formatted csv
    Output: None.
    """
    insert_statement = "INSERT INTO {} (start_date, start_time, miles, hours, minutes) VALUES ".format(table_name)
    with open(path_to_csv_input, 'r') as infile:
        count = 0
        csvreader = csv.reader(infile)
        header = next(csvreader)
        for line in csvreader: #exclude the header row
            insert_statement += "('{}', '{}', {:.1f}, {}, {}), ".format(line[0], line[1], float(line[2]), int(line[3]), int(line[4].rstrip()))    
            count += 1
    insert_statement = insert_statement[:-2] + ";" #strip the extra comma and space; add a semicolon
    if verbose:
        print("{} rows written from {} into database.".format(count, path_to_csv_input))
    cursor = db_conn_obj.cursor()
    cursor.execute(insert_statement)
    db_conn_obj.commit()
    cursor.close()
    return None