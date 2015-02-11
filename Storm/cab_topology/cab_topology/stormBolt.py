# This program creates a storm bolt that filters the incoming tuples based on occupancy
# It stores the unoccupied cab details into HBase which are then shown on UI. The HBase
# table is refreshed every 5 seconds using a tick tuple.
#import logging
from pyleus.storm import SimpleBolt
import happybase
import json

connection = happybase.Connection('54.67.126.144') 

minuteTbl = connection.table('avlbl_Cabs')

class firstBolt(SimpleBolt):
    OUTPUT_FIELDS = ['cabs']
    
    def initialize(self):
        self.unoccCabs = {}        

    def process_tuple(self, tup):
        result, = tup.values
        cabID, lat, lng, occ, timestamp = result.split(" ")
        
        if (occ != '\N'): # check to ensure that there are no null values
            if int(occ) == 0:  
                self.unoccCabs[cabID] = {'c:lat':lat, 'c:lng':lng} # add unoccupied cab to table
            else:
	       if int(occ) == 1:
                  if (cabID in self.unoccCabs.keys()):
                      del self.unoccCabs[cabID]
                      minuteTbl.delete('StormData', columns=['c:' + cabID]) # remove cab from table if it is now occupied
     
    def process_tick(self):
        cur_cabs = self.unoccCabs
	colDict = {}
        for key, val in cur_cabs.iteritems():
	    colDict['c:' + key] = json.dumps(val) # Add currently available cabs to HBase
        minuteTbl.put('StormData', colDict)

if __name__ == '__main__':
    firstBolt().run()

