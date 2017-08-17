'''
Created on Mar 16th, 2016

@author: nathan
'''
from logger import myLogger
import os.path
import re
import yaml

class DatManager(object):
    """Take ParseNXS objects and handle averaging and subtracting
    
    The class is instantiated without any arguments and ParseNXS objects
    are added as the come in. The DatManager tries to work out when to
    average files together and what to subtract from what.
    """
    
    '''
    Constructor
    '''
    def __init__(self):
        ###import config file
        self.pypline_dir = os.path.dirname(os.path.realpath(__file__))
        with open(self.pypline_dir+'/config.yaml', 'r') as ymlfile:
            self.myconfig = yaml.load(ymlfile)

        ###connect to the logger
        self.logger = myLogger(os.path.basename(__file__))

        ###set some parameters
        self.previous_scan_type = None
        self.previous_scan_name = None
        self.type = 'dat_manager'

    def AddParsedNXS(self, parsednxs):
        return_type = ''
        try:
            if not parsednxs.type == 'nxs':
                raise TypeError('DatManager.AddParsedNXS needs a ParsedNXS object')
            ############################
            ##    A ROBOT BUFFER      ##
            ############################
            if parsednxs.scan_type == 'Robot Buffer':
                #return_type = 'repeat robot buffer'
                ### SINCE USERS SPECIFY RUN ORDER THE WAY OF RECOGNISING REPEAT BUFFERS HAS BEEN FAILING, THIS IS A TEMP FIX 
                #if self.previous_scan_type == 'Robot Sample': #A BUFFER AFTER A ROBOT SAMPLE MEANS A REPEAT BUFFER
                #    self.logger.info('dat_manager detected a repeat robot buffer')
                #    return_type = 'repeat robot buffer'
                #else: #A ROBOT BUFFER AFTER ANYTHING BUT A ROBOT SAMPLE IS A NEW BUFFER
                #    self.logger.info('dat_manager detected a new robot buffer')
                    return_type = 'new buffer'

            ############################
            ##    A ROBOT SAMPLE      ##
            ############################
            elif parsednxs.scan_type == 'Robot Sample':
                self.logger.info('dat_manager detected a new robot sample')
                return_type = 'new robot sample'

            ############################
            ##     A SEC BUFFER       ##
            ############################
            elif parsednxs.scan_type == 'SEC Buffer':
                ############################
                ##  A REPEAT SEC BUFFER   ##
                ############################
                if self.previous_scan_type == parsednxs.scan_type and self.previous_scan_name == parsednxs.descriptive_title:
                    self.logger.info('dat_manager detected a repeat SEC buffer')
                    return_type = 'repeat sec buffer'
                ############################
                ##    A NEW SEC BUFFER    ##
                ############################
                else:
                    return_type = 'new sec buffer'

            ############################
            ##     A SEC SAMPLE       ##
            ############################
            elif parsednxs.scan_type == 'SEC Sample':
                self.logger.info('Detected a new SEC sample, blanking')
                return_type = 'new sec sample'

            ############################
            ## A MANUAL SHOT OR SCAN  ##
            ############################
            else:
                self.logger.info('Instrument scan or manual collection, will ignore')
                return_type = 'manual collection or scan'

            self.previous_scan_type = parsednxs.scan_type
            self.previous_scan_name = parsednxs.descriptive_title
            return return_type
        except Exception as ex:
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            self.logger.error(message)
            return False        


if __name__ == '__main__':
    from raw_dat import RawDat
    from database import Database
    from visit_id import VisitID
    from nxs import ParseNXS
    dat_manager = DatManager()
    visit = VisitID('sw14620-1')
    database = Database()
    first_robot_buffer = '/dls/b21/data/2016/sw14620-1/b21-59102.nxs'
    robot_sample = '/dls/b21/data/2016/sw14620-1/b21-59103.nxs'
    repeat_robot_buffer = '/dls/b21/data/2016/sw14620-1/b21-59104.nxs'
    first_sec_buffer = '/dls/b21/data/2016/sw14620-1/b21-59116.nxs'
    repeat_sec_buffer = '/dls/b21/data/2016/sw14620-1/b21-59117.nxs'
    sec_peak = '/dls/b21/data/2016/sw14620-1/b21-59145.nxs'
    parsednxs = ParseNXS(database, visit, first_robot_buffer)
    for datfile in parsednxs.ReturnDatFiles():
        parsednxs.AddDatData(RawDat(datfile))
    print '-----------------------------------------'
    print 'THIS SHOULD READ "new buffer":'
    print dat_manager.AddParsedNXS(parsednxs)

    parsednxs = ParseNXS(database, visit, robot_sample)
    for datfile in parsednxs.ReturnDatFiles():
        parsednxs.AddDatData(RawDat(datfile))
    print '-----------------------------------------'
    print 'THIS SHOULD READ "new robot sample":'
    print dat_manager.AddParsedNXS(parsednxs)

    parsednxs = ParseNXS(database, visit, repeat_robot_buffer)
    for datfile in parsednxs.ReturnDatFiles():
        parsednxs.AddDatData(RawDat(datfile))
    print '-----------------------------------------'
    print 'THIS SHOULD READ "repeat robot sample":'
    print dat_manager.AddParsedNXS(parsednxs)

    
    print 'finished cleanly'
         
