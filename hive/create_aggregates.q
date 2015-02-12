-- Script for creating hour of day, daily, day of week and monthly aggregates

DROP TABLE IF EXISTS ref_tbl;
DROP TABLE IF EXISTS hour_agg;
DROP TABLE IF EXISTS day_agg;
DROP TABLE IF EXISTS dow_agg;
DROP TABLE IF EXISTS month_agg;

-- Convert the time stamp into day, month, year
CREATE TABLE master_tbl AS SELECT uid as cabID, year(from_unixtime(time)) AS year, month(from_unixtime(time)) AS month,
day(from_unixtime(time)) AS day, hour(from_unixtime(time)) AS hour, from_unixtime(time, 'EEE') as dow, occ, pickup, dropoff 
FROM final_tbl;

-- Hourly aggregations
CREATE TABLE hour_agg
AS SELECT * FROM
(SELECT hour, day, month, year, dow, AVG(occ) as occ, SUM(pickup) as pickups, SUM(dropoff) as dropoffs
FROM ref_tbl 
GROUP BY year, month, dow, day, hour 
ORDER BY hour ASC, day ASC, month ASC, year ASC ) a;

-- Daily aggregations
CREATE TABLE day_agg
AS SELECT * FROM
(SELECT day, month, year, AVG(occ) as occ, SUM(pickup) as pickups, SUM(dropoff) as dropoffs
FROM ref_tbl 
GROUP BY year, month, day 
ORDER BY day ASC, month ASC, year ASC ) b;

-- Day of Week aggregations
CREATE TABLE dow_agg
AS SELECT * FROM
(SELECT year, month, dow, hour, AVG(occ) as occ, AVG(pickups) as pickups, AVG(dropoffs) as dropoffs
FROM hour_agg 
GROUP BY year, month, dow, hour
ORDER BY dow ASC, month ASC, year ASC ) c;

-- Monthly aggregations
CREATE TABLE month_agg
AS SELECT * FROM
(SELECT month, AVG(occ) as occ, SUM(pickup) as pickups, SUM(dropoff) as dropoffs 
FROM ref_tbl GROUP BY year, month 
ORDER BY month ASC, year ASC ) d;



