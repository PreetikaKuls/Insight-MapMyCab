vehicle_data = LOAD '/user/ubuntu/AggOutput/' USING PigStorage(',') AS (uid:chararray, Occupancy:float);
-- by_driver = GROUP vehicle_data BY uid;
-- occ_avg = FOREACH by_driver GENERATE
   -- group as uid,
   -- AVG(Occupancy) as averageOcc;



copy = STORE vehicle_data INTO 'hbase://finalTable'
       using org.apache.pig.backend.hadoop.hbase.HBaseStorage(
       'user:uid, user:occupancy');
