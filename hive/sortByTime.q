-- Load the static data set organized by cab so that it can be sorted on time to create real time stream
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

INSERT OVERWRITE DIRECTORY 'hdfs:///user/SortedData' SELECT * FROM raw_table ORDER BY time ASC;

-- Store sorted data in a master table
CREATE TABLE masterTbl(
    uid STRING,
    lat FLOAT,
    long FLOAT,
    occ INT,
    time INT
    )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
STORED AS TEXTFILE
LOCATION '/user/SortedData'
