DROP TABLE final_table;
CREATE EXTERNAL TABLE final_table(
    uid STRING,
    latitude FLOAT,
    longitude FLOAT,
    occ INT,
    time INT
    )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
STORED AS TEXTFILE
LOCATION '/user/ubuntu/source';

CREATE TABLE IF NOT EXISTS new_hbase_table(rowkey STRING, x FLOAT)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf:user");

SET hive.hbase.bulk=true;

INSERT OVERWRITE TABLE new_hbase_table
SELECT uid, avg(occ) from final_table group by uid having avg(occ) > 0.5;

--Comments (ignore)
-- LOAD DATA INPATH '/user/ubuntu/test' OVERWRITE INTO TABLE final_table;

--INSERT OVERWRITE DIRECTORY '/user/ubuntu/AggOutput'
--    ROW FORMAT DELIMITED ;
--    FIELDS TERMINATED BY ' ';
--      STORED AS TEXTFILE 
--SELECT uid, avg(occ) from final_table group by uid having avg(occ) > 0.5;
