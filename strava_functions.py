import requests
import json
import datetime
import csv
import pandas as pd
import psycopg2
from sqlalchemy import create_engine


def obtain_api_access_info(path_to_json):
    '''
    Input: path string to json document containing  API access info
    Reads contents from JSON document into a dictionary.
    Output: Dictionary containing key-value pairs from the JSON file.
    '''
    with open(path_to_json, "r") as infile:
        apikeys = json.load(infile)
    return apikeys

''' Strava-specific Functions '''

def refresh_access_token(access_data):
    '''
    Input: JSON of all api and database info (i.e. output of obtain_api_access_info
    The function uses the refresh token to obtain a new access token.
    Output: A new dictionary of refreshed token values, with expiration date.
    '''
    response = requests.post(url = access_data["strava"]['auth_url'], \
                             data = { \
                                    'client_id':access_data["strava"]['client_id'],\
                                    'client_secret':access_data["strava"]['client_secret'],\
                                    'grant_type':'refresh_token',\
                                    'refresh_token':access_data["strava"]['refresh_token']
                                    } 
                            )
    response_json = response.json()
    new_api_keys = access_data #copy existing values into a new dict
    for key, val in response_json.items():
        new_api_keys["strava"][key] = val #update relevant values in the new dict
    return new_api_keys
    
def check_if_expired(path_to_api_keys, verbose=True):
    '''
    Input: File path to strava api access data.
    Calls the refresh_access_key function if Access token is expired, and refreshes if needed.
    Output: None (writes to existing api key file provided as input) 
    '''
    api_keys = obtain_api_access_info(path_to_api_keys)
    if datetime.datetime.fromtimestamp(api_keys["strava"]["expires_at"]) < datetime.datetime.now():
        if verbose:
            print("---- Checking if API Tokens are valid. ----")
            print("Expired token. Refreshing now.")
            print("Expiration Time: {}, Current time: {}".format(\
                                                                 datetime.datetime.fromtimestamp(api_keys["strava"]["expires_at"]),\
                                                                 datetime.datetime.now()))
        new_values = refresh_access_token(api_keys)
        with open(path_to_api_keys, "w") as outfile:
            json.dump(new_values, outfile)
        if verbose:
            print("New Expiration time: {}".format(datetime.datetime.fromtimestamp(new_values["strava"]["expires_at"])))
            print("Tokens within {} have been updated. Continuing.".format(path_to_api_keys))
    else:
        if verbose:
            print("---- Checking if API Tokens are valid. ----")
            print("Everything is good. No changes will be made")
    return None

def get_past_runs(path_to_api_keys, start_datetime_string, end_datetime_string, verbose=True):    
    '''
    Input: string path to apikeys, starting datetime and ending "2022-08-12 10:45:32" format
    Pulls (via GET) all forms of running (trail, treadmill, run) between specified date range.
    Output: returns list of dictionary objects
    '''
    if verbose:
        print("---- Running GET request for runs from Strava. ----")
    #pull api keys from json file
    strava_api_access = obtain_api_access_info(path_to_api_keys)["strava"]
    #convert argument times into epoch format for inclusion in GET request
    start_epoch_format = datetime.datetime.strptime(start_datetime_string, '%Y-%m-%d %H:%M:%S').timestamp()
    end_epoch_format = datetime.datetime.strptime(end_datetime_string, '%Y-%m-%d %H:%M:%S').timestamp()
    if verbose:
        print("Obtaining Strava run data from {} to {}".format(start_datetime_string, end_datetime_string))
    #construct the HTTPS target
    check_if_expired(path_to_api_keys, verbose=verbose)
    base_url="https://www.strava.com/api/v3/athlete/activities"
    full_url=base_url+"/?access_token="+strava_api_access['access_token']
    #construct the GET request with parameters of access_token, after, and before
    activities = requests.get(base_url, \
                                   params={\
                                           "access_token":strava_api_access['access_token'],\
                                           "after":start_epoch_format,\
                                           "before":end_epoch_format
                                          }\
                                  )
    list_of_activities = activities.json()
    list_of_runs = []
    #populate list_of_runs with all activities containing "run" or "treadmill" (case-insensitive) somewhere in the type.
    for event in list_of_activities:
	event = json.dumps(event)
        if "run" in event["type"].lower() or "treadmill" in event["type"].lower():
            list_of_runs.append(event)
    ''' This block displays the actual GET URL, but it's obnoxious, so I've commented it out.
    if verbose: #Display URL get request being used
        #remove the access token from the url before printing
        get_url = activities.request.url
        first_equals_index = get_url.index('=', 1)
        first_and_index = get_url.index('&', 1)
        redacted_get_url = '<value-hidden>'.join([get_url[:first_equals_index+1], get_url[first_and_index:]])
        #return the HTTP Response message in json format and the number of runs in the list
        print("Here's the URL we'll send our GET request to:\n{}".format(redacted_get_url))
        print("There are {} total runs logged in the output list.".format(len(list_of_runs)))
    '''
    return list_of_runs

def generate_runs_df(strava_output_json, verbose=True):
    """
    Input: raw json output from a Strava api pull - json formatted list of dictionaries
    This function transforms json output into a pandas dataframe ready for inserting 
    into the postgres database.
    Output: Pandas dataframe
    """
    if verbose:
        print("---- Formatting strava output into a Dataframe suitable for database loading. ----")
    data = {'start_date':[], 'start_time':[], 'miles':[], \
        'hours':[], 'minutes':[]}

    for run in strava_output_json:
        data['start_date'].append(run['start_date_local'].split('T')[0])
        data['start_time'].append(run['start_date_local'].split('T')[1].rstrip('Z'))
        data['miles'].append(round(run['distance'] / 1609, 1)) #in miles
        total_minutes = round(run['elapsed_time']//60, 1)
        hours = round(total_minutes//60, 1)
        minutes = total_minutes % 60
        data['hours'].append(hours)
        data['minutes'].append(minutes)
    df = pd.DataFrame(data)
    df.sort_values(by=['start_date','start_time'],ascending=True)
    return df
    
''' Postgres Database-specific function '''