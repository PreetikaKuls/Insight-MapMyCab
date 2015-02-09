--DROP TABLE idleTrips;
CREATE TABLE IF NOT EXISTS idleTrips (
    tripId STRING,
    day INT,
    month INT,
    year INT,
    duration INT)

STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key, c:day, c:month, c:year, c:duration");

INSERT OVERWRITE TABLE idleTrips SELECT tripId, day, month, year, duration FROM idle_trips;

