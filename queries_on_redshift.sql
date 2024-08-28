-- SQL queries on the data in Redshift

SELECT *
FROM stl_load_errors
LIMIT 10;

-- Select the data
SELECT
    *
FROM
    strava_activities
LIMIT
    10;

-- Count of different activity types
SELECT
    activity_type,
    COUNT(*) AS activity_count
FROM
    strava_activities
GROUP BY
    activity_type
ORDER BY
    activity_count DESC;

-- Average distance and speed for each activity type
SELECT
    activity_type,
    AVG(activity_distance) AS avg_distance,
    AVG(average_speed) AS avg_speed
FROM
    strava_activities
GROUP BY
    activity_type;

-- 7 day moving average of average speed
SELECT
    start_date,
    average_speed,
    AVG(average_speed)
    OVER (
        ORDER BY start_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_speed
FROM
    strava_activities;

-- Total number of activities and kudos
SELECT
    COUNT(activity_id) AS total_num_of_activity,
    SUM(kudos_count) AS total_num_of_kudos
FROM
    strava_activities;

