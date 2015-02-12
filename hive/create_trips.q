-- This query creates a table of trips across all the cabs. A unique trip ID is assigned to each trip
-- Trips are detected based on pickup events

DROP TABLE IF EXISTS tripTbl;

CREATE TABLE IF NOT EXISTS tripTbl(
    tripId STRING,
    uid STRING,
    year INT,
    month INT,
    day INT,
    time INT,
    trip INT,
    idle INT,
    lat FLOAT,
    long FLOAT,
    duration INT
    )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' ';

-- Use windowing function to subtract subsequent time intervals (between pickup and dropoff) to 
-- generate trip times and idle times

INSERT OVERWRITE TABLE tripTbl
SELECT tripId, uid, year(from_unixtime(time)) AS year, month(from_unixtime(time)) AS month, day(from_unixtime(time)) AS day, time, trip, idle, lat, long, duration FROM (
    SELECT tripId, uid, time, trip, idle, lat, long, LEAD(time, 1, 0)
    OVER (PARTITION BY uid ORDER BY time) - time 
    AS duration FROM ( 
        SELECT concat(uid, '_', time) AS tripId, uid, time, pickup AS trip, dropoff AS idle, lat, long
        FROM final_tbl
        WHERE pickup = 1 OR dropoff = 1) t) view
    WHERE duration > 0;
