-- Final Table for storing original and generated columns

DROP TABLE final_tbl;
CREATE TABLE IF NOT EXISTS final_tbl(
    uid STRING,
    time INT,
    occ INT,
    pickup INT,
    dropoff INT,
    lat FLOAT, 
    long FLOAT
    );

-- Generate pickup and dropoff events, pickup = occupancy flip from 0 to 1, dropoff = occupancy flip from 1 to 0

INSERT OVERWRITE TABLE final_tbl
SELECT uid, time, occ, 
CASE WHEN pick = -1 THEN 1
ELSE 0 END AS pickup,
CASE WHEN pick = 1 THEN 1
ELSE 0 END AS dropoff, lat, long
FROM (
SELECT uid, time, occ, LAG(occ, 1, 0)
OVER (PARTITION by uid ORDER BY time) - occ AS pick, lat, long
FROM masterTbl) a;


