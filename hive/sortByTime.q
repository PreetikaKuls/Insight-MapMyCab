DROP TABLE raw_table;
CREATE EXTERNAL TABLE raw_table(
    uid STRING,
    lat FLOAT,
    long FLOAT,
    occ INT,
    time INT
   )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
STORED AS TEXTFILE
LOCATION '/user/TestData';

INSERT OVERWRITE DIRECTORY 'hdfs:///user/TestData/SortedByTime' SELECT * FROM raw_table ORDER BY time ASC;

