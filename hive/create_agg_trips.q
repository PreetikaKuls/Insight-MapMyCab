--Find max idle time of cab per day, this would help in identifying a potential shift

DROP TABLE IF EXISTS max_idle_dur;
CREATE TABLE max_idle_dur (
    uid STRING,
    month INT,
    year INT,
    day INT,
    maxDur INT
    );

-- Store the maximum durations for idle times per day and by cab into a table
INSERT OVERWRITE TABLE max_idle_dur
SELECT uid, month, year, day, MAX(duration) as maxDur
FROM tripTbl
WHERE idle = 1
GROUP BY uid, month, year, day;

DROP TABLE IF EXISTS idle_trips;
CREATE TABLE idle_trips (
    tripId STRING,
    day INT,
    month INT,
    year INT,
    duration INT
   );

-- Select the tripIds with the max duration of idle times to identify cab and its shift
INSERT OVERWRITE TABLE idle_trips
SELECT tripId, day, month, year, duration FROM tripTbl
WHERE tripTbl.duration IN (
SELECT maxDur FROM max_idle_dur WHERE max_idle_dur.uid = tripTbl.uid AND max_idle_dur.day = tripTbl.day);

DROP TABLE IF EXISTS avg_trip_dur;
CREATE TABLE avg_trip_dur (
    year INT,
    month INT,
    dow STRING,
    avg_dur FLOAT);

-- Find out the average trip duration for a day of week
INSERT OVERWRITE TABLE avg_trip_dur 
SELECT e.* FROM 
(SELECT year, month, from_unixtime(time, 'EEE') as dow, avg(duration) AS avg_dur
FROM tripTbl GROUP BY year, month, dow) e
WHERE trip = 1;
GROUP BY year, month, dow;

