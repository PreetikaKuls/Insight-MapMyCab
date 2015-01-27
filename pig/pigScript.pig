vehicle_data = LOAD '/home/ubuntu/mapmycab/pig/cabFile.txt' USING PigStorage(',') AS (uid:chararray, latitude:float, longitude:float, occupancy:int, timestamp:float);





-- by_driver = GROUP vehicle_data BY uid;
-- occ_avg = FOREACH by_driver GENERATE
   -- group as uid,
   -- AVG(Occupancy) as averageOcc;



copy = STORE vehicle_data INTO 'hbase://finalTable'
       using org.apache.pig.backend.hadoop.hbase.HBaseStorage(
       'user:uid, user:occupancy');
