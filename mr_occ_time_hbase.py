from mrjob.job import MRJob
from mr_occ_time_job import OccTimeJob
import datetime
import happybase
import sys, inspect

hbase = happybase.Connection('localhost')
#hbase.create_table(
 #   'occ_by_time',
  #  {'occ': dict(),
  #  }
#)
hbase_occ_table = hbase.table('occ_by_time')
hbase_occ_table_batch = hbase_occ_table.batch(batch_size=10)

if __name__ == '__main__':
	sys.argv += ['--jobconf', 'mapred.job.name=' + inspect.getmodulename(__file__)]
	mr_job = OccTimeJob(args=sys.argv[1:])
	with mr_job.make_runner() as runner:
		runner.run()
		
		total_entries = 0
		for line in runner.stream_output():
			key, value = mr_job.parse_output_line(line)
			hbase_occ_table_batch.put(str(key), {'occ:total':str(value)})
			total_entries += 1
		print "Time entries processed: %s" % total_entries
		hbase_occ_table_batch.send()
