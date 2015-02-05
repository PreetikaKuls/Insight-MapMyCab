import logging
from pyleus.storm import SimpleBolt
import happybase
import json

connection = happybase.Connection('54.67.126.144') #Running HBase, thrift this will be run by supervisor

minuteTbl = connection.table('avlbl_Cabs')
log = logging.getLogger('test')

class firstBolt(SimpleBolt):
    OUTPUT_FIELDS = ['test']
    
    def initialize(self):
        self.unoccCabs = {}        

    def process_tuple(self, tup):
       # if pyleus.storm.is_tick(tup):
        #    log.debug("RECEIVED TICK TUPLE")
         #   self.ack(tup)
         #   return
        result, = tup.values
        
        log.debug("Received tuple " + str(result))
        
        cabID, lat, lng, occ, timestamp = result.split(" ")
        
        if (occ != '\N'): # check to ensure that there are no null values 
            if int(occ) == 0:
                log.debug("Adding new cab " + cabID)
                self.unoccCabs[cabID] = {'c:lat':lat, 'c:lng':lng}
            else:
	       if int(occ) == 1:
                  log.debug("Found " + cabID + " with occ " + occ)
                  log.debug("keys", json.dumps(self.unoccCabs.keys()))
                  if (cabID in self.unoccCabs.keys()):
                      del self.unoccCabs[cabID]
                      minuteTbl.delete('StormData', columns=['c:' + cabID])
                      log.debug("Deleting" + cabID + " with occ " + occ)
        #self.ack(tup)
     
    def process_tick(self):
        cur_cabs = self.unoccCabs
	colDict = {}
        for key, val in cur_cabs.iteritems():
	    colDict['c:' + key] = json.dumps(val)
	#log.debug("Storing into HBase", colDict)
        minuteTbl.put('StormData', colDict)
        
        #minuteTbl.put('StormData', {'c:' + key: json.dumps(val)})
        #self.unoccCabs = {}
             

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/home/ec2-user/test_pyleus_log.log',
        format="%(message)s",
        filemode='a',
    )
    firstBolt().run()

