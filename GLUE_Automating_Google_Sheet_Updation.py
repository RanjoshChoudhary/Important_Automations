# 👍 To run the given script in AWS Glue and schedule it to run every afternoon, 
# We can not directly run the script as is as glue runs in a serverless environment and uses PySpark
# We have to adapt the logic to AWS Glue 

# Python version compatible with our script - 
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
    return pd.read_sql(query, connection)
    
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
    queries_and_sheets = [
        ("""
-- has our events cta etc to see clicks and tag respectively [[data_playground.ranjosh_detail_pages]]
WITH B AS
(
SELECT
CASE
-- CODING
WHEN attr_current_path ILIKE '%coding%' AND attr_cta='close' AND intent_medium='HOME_FEED_MATH_PLUS_CARD' THEN 'CODING'

WHEN attr_current_path ILIKE '%science%' AND attr_cta='close' AND intent_medium='HOME_FEED_MATH_PLUS_CARD' THEN 'SCIENCE'

WHEN attr_current_path ILIKE '%english%' AND attr_cta='close' AND intent_medium='HOME_FEED_MATH_PLUS_CARD' THEN 'ENGLISH'

WHEN attr_current_path ILIKE '%/sat%' AND attr_cta='close' AND intent_medium='HOME_FEED_MATH_PLUS_CARD' THEN 'SAT'
END AS Event_Tags
,*
FROM
data_playground.ranjosh_pages
  )

-- SELECT * from B
, C as (
SELECT
event_ts_ist::date as date
,derived_region
,Event_Tags
,COUNT(*)  AS clicks
,COUNT(DISTINCT parent_id_1)  AS nr_of_parents
,min(event_ts_ist) AS first_event_date
,max(event_ts_ist) as recent_event_date
FROM B
where Event_Tags IS NOT NULL
GROUP BY 1, CUBE(2,3)
-- ORDER BY 2, 4 DESC
  )
SELECT
date
,COALESCE(derived_region,'Overall') as country
,COALESCE(Event_Tags,'Overall') as t
,clicks
,nr_of_parents
,first_event_date
,recent_event_date
from C
ORDER BY date DESC
""", "data_update"),
        (
         """
         -- has our events cta etc to see clicks and tag respectively [[data_playground.ranjosh_detail_pages]]
WITH B AS
(
SELECT
CASE
-- CODING
WHEN attr_current_path ILIKE '%coding%' AND attr_cta='close' AND intent_medium='HOME_FEED_MATH_PLUS_CARD' THEN 'CODING'

WHEN attr_current_path ILIKE '%science%' AND attr_cta='close' AND intent_medium='HOME_FEED_MATH_PLUS_CARD' THEN 'SCIENCE'

WHEN attr_current_path ILIKE '%english%' AND attr_cta='close' AND intent_medium='HOME_FEED_MATH_PLUS_CARD' THEN 'ENGLISH'

WHEN attr_current_path ILIKE '%/sat%' AND attr_cta='close' AND intent_medium='HOME_FEED_MATH_PLUS_CARD' THEN 'SAT'
END AS Event_Tags
,*
FROM
data_playground.ranjosh_pages
  )

-- SELECT * from B
, C as (
SELECT
  DATE_TRUNC('week', event_ts_ist::date) AS week
,derived_region
,Event_Tags
,COUNT(*)  AS clicks
,COUNT(DISTINCT parent_id_1)  AS nr_of_parents
,min(event_ts_ist) AS first_event_date
,max(event_ts_ist) as recent_event_date
FROM B
where Event_Tags IS NOT NULL
GROUP BY 1, CUBE(2,3)
-- ORDER BY 2, 4 DESC
  )
SELECT
week
,COALESCE(derived_region,'Overall') as country
,COALESCE(Event_Tags,'Overall') as t
,clicks
,nr_of_parents
,first_event_date
,recent_event_date
from C
         """, "data_weekly"
        )
    ]

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

