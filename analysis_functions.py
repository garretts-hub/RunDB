from pg_functions import display_db_as_df, perform_sql_query, upload_from_csv
import pandas as pd
import datetime
import seaborn as sns
import math
import matplotlib.pyplot as plt

def generate_weekly_summary(db_connection_obj, table_name, start_date_str, end_date_str, display_heatmap=True, verbose=True):
    '''
    Input: db_connection object, string name of table, string start date and end date (YYYY-MM-DD). Can optionally display a heatmap.
    Output: Pandas dataframe with the following columns: ["Week of:", "Mon", "Tues", "Weds", "Thurs", "Fri", "Sat", "Sun", "Total"].
    "Week Of" is the first monday of the week, total is the sum of the previous 7 columns. Function will pull data from the entire week
    containing the first day, and the entire week of the last day, even if it's not fully complete or populated.
    '''
    #Convert the input strings into datetime objects
    start_obj = datetime.datetime.strptime(start_date_str, "%Y-%m-%d")
    end_obj = datetime.datetime.strptime(end_date_str, ("%Y-%m-%d")) 
    #### find the nearest monday, if the start_date is not already a monday
    start_weekday = start_obj.weekday()
    first_monday_obj = start_obj - datetime.timedelta(days=start_weekday)
    first_monday_str = first_monday_obj.strftime("%Y-%m-%d")
    #### find monday of the week of the end-date
    end_weekday = end_obj.weekday()
    last_monday_obj = end_obj - datetime.timedelta(days=end_weekday)
    last_monday_str = last_monday_obj.strftime("%Y-%m-%d")
    last_sunday_obj = last_monday_obj + datetime.timedelta(days=6)
    last_sunday_str = last_sunday_obj.strftime("%Y-%m-%d")
    ### Make a list of the starting monday object for each week of interest
    total_days_difference = (last_monday_obj - first_monday_obj).days
    total_weeks = math.ceil(total_days_difference/7) + 1
    list_of_mondays = [first_monday_obj + datetime.timedelta(weeks=i) for i in range(total_weeks)] #list of datetime objects
    #Initialize the dataframe that we'll use to generate the graphic, starting with column headers
    df = pd.DataFrame(columns=["Week of:", "Mon", "Tues", "Weds", "Thurs", "Fri", "Sat", "Sun", "Total"])
    #Run an SQL query to read in all runs from start-monday (inclusive) through the last sunday inclusive, into data_df
    data_df = display_db_as_df(db_connection_obj, table_name, first_monday_str, last_sunday_str, verbose=verbose)
    data_df = data_df.astype({"start_date":"string"}) #convert this column to a string (originally an object) to use query()
    day_dict = {0:"Mon", 1:"Tues", 2:"Weds", 3:"Thurs", 4:"Fri", 5:"Sat", 6:"Sun"}
    #Populate a dictionary for each week based on the columns declared for df
    for monday in list_of_mondays:
        row_dict = {}
        week_of_string = monday.strftime("%b %d")
        row_dict["Week of:"] = week_of_string #string value for MMM dd
        weekly_total = 0
        for i in range(7):
            day_obj = monday + datetime.timedelta(days=i)
            day_string = day_obj.strftime("%Y-%m-%d")
            daily_mileage = round(data_df.query("start_date == @day_string")['miles'].sum(), 1)
            row_dict[day_dict[i]] = daily_mileage
            weekly_total += daily_mileage
        row_dict["Total"] = weekly_total
        row_df = pd.DataFrame.from_records([row_dict])
        df = pd.concat([df, row_df])
    df = df.set_index("Week of:")
    if display_heatmap:
        lowest_mileage = df['Total'].min()
        highest_mileage = df['Total'].max()
        height = 0.1*df.size
        plt.figure(figsize=(12,height))
        ax = sns.heatmap(df, cmap="crest", robust=True, 
                         annot=True, linewidths=.5, linecolor="grey", vmin=lowest_mileage, 
                         vmax=highest_mileage, annot_kws={'size':15}, 
                         cbar_kws={'shrink':.8, 'label':'Weekly Mileage'})
        ax.set_yticklabels(ax.get_yticklabels(), rotation=0)
        ax.xaxis.tick_top()
        plt.title("Weekly Mileage Summary")
        plt.show()
    return df