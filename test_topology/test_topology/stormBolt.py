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
        log.debug("Received this line " + cabID + " " + lat + "  " + lng + " " + occ + " " + timestamp)
        
        if (occ != '\N'):
            if int(occ) == 0:
                if cabID not in self.unoccCabs:
                    log.debug("Adding new cab " + cabID)
                    self.unoccCabs[cabID] = [lat, lng, occ, timestamp]
 
                    # metrics = {'lat':lat, 'lng':lng}
                    # minuteTbl.put('StormData', {'c:' + cabID: json.dumps(metrics)})

                    log.debug(str(self.unoccCabs[cabID]) + " " + cabID + " " + timestamp + " " + occ)
            elif cabID in self.unoccCabs:
               log.debug("Deleting " + cabID + " with occ " + occ)
               del self.unoccCabs[cabID]

        #self.ack(tup)
     
    def process_tick(self):
        cur_cabs = self.unoccCabs
        for key, val in cur_cabs.iteritems():
            minuteTbl.put('StormData', {'c:' + key: json.dumps(val)})
        self.unoccCabs = {}
             

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/home/ec2-user/test_pyleus_log.log',
        format="%(message)s",
        filemode='a',
    )
    firstBolt().run()

