DROP TABLE hbase_dow_table;

CREATE TABLE IF NOT EXISTS hbase_dow_table(
    rowkey STRING,
    hour STRING,
    occupancy FLOAT,
    pickups FLOAT,
    dropoffs FLOAT,
    distance FLOAT)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key, c:hour, c:occ, c:pickups, c:dropoffs, c:distance");

INSERT OVERWRITE TABLE hbase_dow_table SELECT CONCAT(year,'_',month,'_',dow, '-',hour), hour, occ, pickups, dropoffs, dist FROM dow_agg;                            
