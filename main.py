import pytz
import boto3
import urllib3
import requests
import pandas as pd
import pandera as pa
from io import StringIO
from pytz import timezone
from pandera import Column, Check
from logging import raiseExceptions
from pandera.typing import DataFrame, Series


def fetch_data_from_api():
    # Fetches the activities from Strava using its API
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    auth_url = "https://www.strava.com/oauth/token"
    activities_url = "https://www.strava.com/api/v3/athlete/activities"

    payload = {
        'client_id': "XXXXXX",
        'client_secret': 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
        'refresh_token': 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
        'grant_type': "refresh_token",
        'f': 'json'
    }

    try:
        print("Requesting Token...\n")
        res = requests.post(auth_url, data=payload, verify=False)
        access_token = res.json()['access_token']
        print("Access Token = {}\n".format(access_token))
    except requests.exceptions.RequestException as e:
        print(f"Error occurred: {e}")

    header = {'Authorization': 'Bearer ' + access_token}
    param = {'per_page': 200, 'page': 1}

    try:
        strava_activities = requests.get(activities_url, headers=header, params=param).json()
    except requests.exceptions.RequestException as e:
        print(f"Error occurred while fetching data: {e}")

    return strava_activities

def convert_to_df(strava_activities):
    # Selects particular columns and converts to DataFrame
    cols = ['name', 'upload_id', 'type', 'distance', 'moving_time',
            'average_speed', 'max_speed', 'start_date',
            'kudos_count', 'photo_count'
            ]
    strava_activities_to_df = pd.json_normalize(strava_activities)
    strava_activities_df = strava_activities_to_df[cols]

    return strava_activities_df

def standardize_column_names(strava_activities_df):
    # Changes column names to have a standardized column names
    strava_activities_df = strava_activities_df \
        .rename(columns={'name': 'activity_name',
                         'upload_id': 'activity_id',
                         'type': 'activity_type',
                         'distance': 'activity_distance'})

    return strava_activities_df

def extract_time_from_date(strava_activities_df):
    # This function extracts time from date and adds it to a new column
    strava_activities_df['start_date_time'] = strava_activities_df['start_date'].str[11:19]
    strava_activities_df['start_date'] = strava_activities_df['start_date'].str[:10]
    cols = strava_activities_df.columns.tolist()
    start_date_index = cols.index('start_date')
    cols.remove('start_date_time')
    cols.insert(start_date_index + 1, 'start_date_time')
    strava_activities_df = strava_activities_df[cols]

    return strava_activities_df

def standardize_schema(strava_activities_df):
    # Changes dtypes of date and time columns
    strava_activities_df['start_date'] = pd.to_datetime(strava_activities_df['start_date'])
    strava_activities_df['start_date_time'] = (pd.to_datetime(strava_activities_df['start_date_time'],
                                                             utc=True).dt.tz_convert('Asia/Kolkata').
                                                             dt.tz_localize(None))
    #strava_activities_df['start_date_time'] = strava_activities_df['start_date_time'].dt.time

    return strava_activities_df

def quality_checks(strava_activities_df):
    # Data quality checks to make sure there are no null values
    # And checks if activity_id column is unique using Pandera
    schema = pa.DataFrameSchema({
        'activity_name': Column(str, nullable=False),
        'activity_id': Column(int, nullable=False, unique=True),
        'activity_type': Column(str, nullable=False),
        'activity_distance': Column(float, nullable=False),
        'moving_time': Column(int, nullable=False),
        'average_speed': Column(float, nullable=False),
        'max_speed': Column(float, nullable=False),
        'start_date': Column(pa.dtypes.DateTime, nullable=False),
        'start_date_time': Column(pa.dtypes.DateTime, nullable=False),
        'kudos_count': Column(int, nullable=False),
        'photo_count': Column(int, nullable=False)
    })
    try:
        schema.validate(strava_activities_df)
        print("All columns have no null values and activity_id is unique!")
    except pa.errors.SchemaError as e:
        raise ValueError(f"DataFrame Validation Failed: {e}")

    return strava_activities_df

def load_to_s3(strava_activities_df):
    # Loads the DataFrame to S3 bucket
    try:
        s3 = boto3.client("s3",
                          aws_access_key_id='XXXXXXXXXXXXXXXXXXXX',
                          aws_secret_access_key='XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
        csv_buf = StringIO()
        strava_activities_df.to_csv(csv_buf, header=True, index=False)
        csv_buf.seek(0)
        s3.put_object(Bucket='strava-data-transformed', Key='strava_activities.csv', Body=csv_buf.getvalue())
        print("File Loaded Successfully!")
    except Exception as e:
        print(f"Error Loading File to S3: {e}")

def main():
    extract_strava_data = fetch_data_from_api()
    select_specific_columns = convert_to_df(extract_strava_data)
    changed_column_names = standardize_column_names(select_specific_columns)
    time_extracted = extract_time_from_date(changed_column_names)
    schema_change = standardize_schema(time_extracted)
    null_value_check = quality_checks(schema_change)
    load_data = load_to_s3(null_value_check)

    # print(changed_column_names)
    # print(schema_change)
    # print(get_info)
    print(null_value_check)
    print(load_data)

if __name__ == '__main__':
    main()
