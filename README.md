# RunDB
Welcome to RunDB! This project utilizes a Jupyter notebook and a PostgreSQL database to practice data engineering and data science concepts, using your Strava running data to do so.<br>

The `RunDB.ipynb` notebook will allow you to run SQL queries against your database, retrieve your Strava running data via the Strava API, and generate Pandas dataframes to visualize and analyze your mileage! For example, here's an example of a Pandas dataframe of some running data, plus a pretty Seaborn heatmap of that data! This lets you draw conclusions about your performance over the weeks while learning ETL along the way:<br>
<img src="https://github.com/garretts-hub/RunDB/blob/main/images/sample_dataframe_output.PNG" alt="Sample output of a Dataframe with Weekly Running data" title="RunDB DataFrame">
<img src="https://github.com/garretts-hub/RunDB/blob/main/images/sample_heatmap_output.png" alt="Sample output of a Seaborn Heatmap with Weekly Running data" title="RunDB Heatmap" width=auto height=75%>
<br>
Note that for this notebook to work, you'll need:
 - An active Strava account
 - Your credentials to the Strava API (client id, client secret key, access token, refresh token, and expiration times)
   - See Section D of https://developers.strava.com/docs/getting-started/ to find out how to obtain these.
 - a running PostgreSQL database (can be local or remote)



