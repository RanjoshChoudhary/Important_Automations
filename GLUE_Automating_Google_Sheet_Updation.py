# Problem Statement - 
# 👍 To run the given script in AWS Glue and schedule it to run every afternoon, 
# We can not directly run the script as is, as glue runs in a serverless environment and uses PySpark
# We have to adapt the logic to AWS Glue 
# ------------------------------------------------------------------------------------------

# Python version compatible with our script - []
# 👍 AWS GLUE SET UP 👍
#  1] Upload Service Account JSON - 
# Upload your service_account_key.json file to an S3 bucket and specify the path in the script.
# On my desktop - Downloaded it the other day to setup google colab. [elite-epoch.json]

# 2] Create a Python Library Layer
"""
Bundle gspread, oauth2client, redshift_connector, and pandas in a ZIP file.
Upload the ZIP file as a Lambda Layer or attach it as an external Python library in AWS Glue.
"""
# Create a new Glue job and upload your script to the job's script path in S3.
# Set up the Glue job to use your Python library layer.

!pip install redshift_connector gspread gspread_dataframe oauth2client

from google.colab import drive
drive.mount('/content/drive') # Replace this Google Colab-specific functions 

import pandas as pd
import redshift_connector
from gspread_dataframe import set_with_dataframe
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Set up connection to Redshift
def connect_to_redshift():
    return redshift_connector.connect(
        host='cuemath.cmiz7uaqdyex.ap-southeast-1.redshift.amazonaws.com',
        database='cuemath',
        user='biuser',
        password='' 
      # Enter password
    )

# Execute SQL query on Redshift
def execute_query(connection, query):
    return pd.read_sql(query, connection) # Function internally depends on libraries like SQLAlchemy
    """
    pd.read_sql abstracts query execution and data fetching, 
    but it doesn't give you control over the cursor's lifecycle. 
    For environments requiring explicit connection and cursor management, 
    it's better to use the cursor object directly.
    """
# Connect to Google Sheets
def setup_google_sheets():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('/content/drive/MyDrive/elite-epoch-441710-b4-2e8a3b1cba82.json', scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_url('https://docs.google.com/spreadsheets/d/1Y787G0gCLQB0s41m9lw30W9dYjr5uiVx0Qi3X6RBYa0/edit?gid=842200988#gid=842200988')
    return spreadsheet
    
# Update worksheet with data
def update_worksheet(spreadsheet, sheet_name, dataframe):
    try:
        # Attempt to get the existing worksheet
        worksheet = spreadsheet.worksheet(sheet_name)

        # Clear only columns A to G
        range_to_clear = 'A:G'
        worksheet.batch_clear([range_to_clear])
    except gspread.exceptions.WorksheetNotFound:
        # If the worksheet doesn't exist, create it
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="20")

    # Update the specified range with the first 7 columns of the dataframe
    set_with_dataframe(worksheet, dataframe.iloc[:, :7], include_index=False)

def main():
    # Your SQL queries and corresponding sheet names
    queries_and_sheets = [
        (""" -- INSERT SQL QUERY 1 """, "data_update"),
        (""" -- INSERT SQL QUERY 2  """, "data_weekly")
    ]
    # -- Have deleted queries for now , add a normal small one query to check if it is working as intended

    conn = connect_to_redshift()
    spreadsheet = setup_google_sheets()

    for query, sheet_name in queries_and_sheets:
        df = execute_query(conn, query)
        update_worksheet(spreadsheet, sheet_name, df)

    conn.close()

if __name__ == '__main__':
    main()
###############################################################################
# GPTs version of this job 
# Ensure the JSON file is present at the specified path and uploaded if running in a cloud environment
# For AWS Glue, the file should be stored in an S3 bucket, and the script should download it during execution.
# Google Sheets API Permissions - Google Sheet must be shared with the service account email from the JSON file.
import boto3
import redshift_connector
import pandas as pd
from gspread_dataframe import set_with_dataframe
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Set up connection to Redshift
def connect_to_redshift():
    return redshift_connector.connect(
        host='cuemath.cmiz7uaqdyex.ap-southeast-1.redshift.amazonaws.com',
        database='cuemath',
        user='biuser',
        password=''  # Add your password
    )

# Execute SQL query on Redshift
"""
execute_query function is different to reduce dependency on SQL Alchemy
"""
def execute_query(connection, query):
    cursor = connection.cursor()  # Explicitly create a cursor
    cursor.execute(query)  # Execute the SQL query
    columns = [desc[0] for desc in cursor.description]  # Extract column names
    results = cursor.fetchall()  # Fetch all rows
    return pd.DataFrame(results, columns=columns)  # Convert to a DataFrame


# Connect to Google Sheets
def setup_google_sheets():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('path_to_your_service_account_key.json', scope)
    # Add path here of the json file that is uploaded to S3 🚨
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_url('https://docs.google.com/spreadsheets/d/your_google_sheet_id')
    # PASTE GOOGLE SHEET URL HERE 🚨 
    return spreadsheet

# Update worksheet with data
def update_worksheet(spreadsheet, sheet_name, dataframe):
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.batch_clear(['A:G'])
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="20")
    set_with_dataframe(worksheet, dataframe.iloc[:, :7], include_index=False)

def main():
    # Your queries and corresponding sheet names
    # Iterates over a list of queries and updates corresponding sheets in Google Sheets with the query results.
    queries_and_sheets = [
        ("Your Query 1 Here", "data_update"),
        ("Your Query 2 Here", "data_weekly")
    ]
    conn = connect_to_redshift()
    spreadsheet = setup_google_sheets()

    for query, sheet_name in queries_and_sheets:
        df = execute_query(conn, query)
        update_worksheet(spreadsheet, sheet_name, df)

    conn.close()

if __name__ == "__main__":
    main()


"""
TODO---Error Handling:
There is minimal error handling in the script. 
Adding try-except blocks for database and API interactions can make the script more robust.
"""


