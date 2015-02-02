DROP TABLE IF EXISTS dow_agg;
--DROP TABLE IF EXISTS master_table;
--DROP TABLE IF EXISTS time_agg;
--DROP TABLE IF EXISTS hour_agg;
--DROP TABLE IF EXISTS day_agg;
--DROP TABLE IF EXISTS month_agg;

--CREATE TABLE master_table AS SELECT uid as cabID, year(from_unixtime(time)) AS year, month(from_unixtime(time)) AS month, 
--day(from_unixtime(time)) AS day, hour(from_unixtime(time)) AS hour, from_unixtime(time, 'EEE') as DOW, occ, pickup, dropoff, dist from new_table;

--CREATE TABLE hour_agg
--AS SELECT * FROM
--(SELECT hour, day, month, year, DOW, AVG(occ) as occ, SUM(pickup) as pickups, SUM(dropoff) as dropoffs, SUM(dist) as dist FROM master_table GROUP BY year, month, DOW, day, hour ) a;

CREATE TABLE dow_agg
AS SELECT * FROM
(SELECT year, month, DOW, hour, AVG(occ) as occ, AVG(pickups) as pickups, AVG(dropoffs) as dropoffs, AVG(dist) as dist from hour_agg GROUP BY year, month, DOW, hour ) d;  




--CREATE TABLE day_agg
--AS SELECT * FROM (SELECT day, month, year, AVG(occ) as occ, AVG(pickup) as pickups, AVG(dropoff) as dropoffs, AVG(dist) as dist FROM master_table GROUP BY year, month, day ) b;

--CREATE TABLE month_agg
--AS SELECT * FROM
--(SELECT month, AVG(occ) as occ, SUM(pickup) as pickups, SUM(dropoff) as dropoffs, SUM(dist) as dist FROM master_table GROUP BY year, month ) c;


