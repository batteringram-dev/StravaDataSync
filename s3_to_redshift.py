import psycopg2

def connect_to_redshift():
    # This function creates a cursor and parameters to connect to Redshift
    conn = psycopg2.connect(
        host='XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
        port='5439',
        database='stravaactivitiesdb',
        user='awsuser',
        password='XXXXXXXXXXXXXXXXX'
    )
    cur = conn.cursor()

    return conn, cur

def create_table_in_redshift(conn, cur):
    # Creates a table in Redshift with appropriate dtypes
    create_table_query = """
    CREATE TABLE strava_activities (
        activity_name VARCHAR(255),
        activity_id BIGINT,
        activity_type VARCHAR(255),
        activity_distance FLOAT,
        moving_time INTEGER,
        average_speed FLOAT,
        max_speed FLOAT,
        start_date TIMESTAMP,
        start_date_time TIMESTAMP,
        kudos_count INTEGER,
        photo_count INTEGER
    );
    """
    try:
        cur.execute(create_table_query)
        conn.commit()
        print("Created Table In Redshift Successfully!")
    except Exception as e:
        print(f"Failed To Create Table In Redshift: {e}")

def load_from_s3_to_redshift(conn, cur):
    # Loads data to Redshift from S3 using COPY command
    copy_query = """
    COPY strava_activities
    FROM 's3://strava-data-transformed/strava_activities.csv'
    CREDENTIALS 'aws_iam_role=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
    CSV
    IGNOREHEADER 1
    DELIMITER ',';
    """
    try:
        cur.execute(copy_query)
        conn.commit()
        print("Loaded Data To Redshift Successfully!")
    except Exception as e:
        print(f"Failed To Load Data To Redshift: {e}")

def commit_close_connection(conn):
    # This function closes the connection
    conn.close()

def main():
    conn, cur = connect_to_redshift()
    try:
        create_table_in_redshift(conn, cur)
        load_from_s3_to_redshift(conn, cur)
    finally:
        commit_close_connection(conn)

if __name__ == "__main__":
    main()