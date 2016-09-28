#!/home/b21user/nathanc/Pypline3/bin/python
'''
Created on Mar 15th, 2016

@author: nathan
'''
from datetime import datetime
import dateutil.parser
import glob
from logger import myLogger
from nexusformat.nexus import nxload
import os.path
import re
import time
import yaml

class ParseNXS(object):
    """Parses a Nexus file
    
    This class contains functions to parse a nexus format file
    output from the GDA software on the B21 beamline, extract
    some parameters and return a dictionary that can be used
    to upload the information to an SQL database.
    """
    
    '''
    Constructor
    '''
    def __init__(self, database=None, visit=None, nexus_file=None):
        ###import config file
        self.pypline_dir = os.path.dirname(os.path.realpath(__file__))
        with open(self.pypline_dir+'/config.yaml', 'r') as ymlfile:
            self.myconfig = yaml.load(ymlfile)

        ###connect to the logger
        self.logger = myLogger(os.path.basename(__file__))

        ###set some parameters
        try:
            if visit.type == 'visit':
                self.visit = visit
            else:
                raise TypeError('ParseNXS needs a visit instance')
            if database.type == 'database':
                self.database = database
            else:
                raise TypeError('ParseNXS needs a database instance')
        except Exception as ex:
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            self.logger.error(message)
            
        self.type = 'nxs'
        self.hplc_pattern = re.compile('\ASECSAXS ')
        self.hplc_sample_pattern = re.compile(' Peak [0-9]+\Z')
        self.robot_sample_pattern_start = re.compile('\ASample: ')
        self.robot_sample_pattern_end = re.compile(' \(Location {1,3}I{1,3} [A-H]  ?[1-9]?[0-2]?\)\Z')
        self.robot_buffer_pattern = re.compile('Buffer for next and preceding sample measurement')

        self.nexus_file = str(nexus_file)
        self.scan_command = ''
        self.descriptive_title = 'Instrument Scan'
        self.scan_type = 'Instrument Scan'
        self.outfile_prefix = 'Instrument_Scan'
        self.file_code = 0
        self.dat_dir = ''
        self.number_of_exposures = 1
        self.exposure_time = 1.0
        self.file_time = ''
        self.visit_id = ''

        self.dat_data = []

        try:
            if not os.path.isfile(self.nexus_file) and self.nexus_file[-4:] == '.nxs':
                raise IOError(str(self.nexus_file)+' does not exist or is not of type .nxs')
            try:
                mynxs = nxload(self.nexus_file)
                self.scan_command = str(mynxs.entry1.scan_command.nxdata)
                self.file_code = int(mynxs.entry1.entry_identifier.nxdata)
                self.dat_dir = os.path.dirname(self.nexus_file)+'/'+self.myconfig['settings']['existing_dat_file_directory']+'/'+self.myconfig['settings']['unsub_dir_pre']+str(self.file_code)+self.myconfig['settings']['unsub_dir_post']+'/'
                self.file_time = dateutil.parser.parse(mynxs.file_time).strftime('%Y/%m/%d_%H:%M:%S')
                self.visit_id = str(mynxs.entry1.experiment_identifier.nxdata)
                if self.scan_command == 'static readout':
                    self.descriptive_title = str(mynxs.entry1.title.nxdata)
                    if re.search(self.hplc_pattern, self.descriptive_title):
                        self.descriptive_title = self.descriptive_title[re.search(self.hplc_pattern, self.descriptive_title).end():]
                        self.scan_type = 'SEC Buffer'
                        if re.search(self.hplc_sample_pattern, self.descriptive_title):
                            self.scan_type = 'SEC Sample'
                    elif re.search(self.robot_buffer_pattern, self.descriptive_title):
                        self.scan_type = 'Robot Buffer'
                    elif re.search(self.robot_sample_pattern_start, self.descriptive_title) and re.search(self.robot_sample_pattern_end, self.descriptive_title):
                        start_index = re.search(self.robot_sample_pattern_start, self.descriptive_title).end()
                        end_index = re.search(self.robot_sample_pattern_end, self.descriptive_title).start()
                        self.descriptive_title = self.descriptive_title[start_index:end_index]
                        self.scan_type = 'Robot Sample'
                    else:
                        self.scan_type = 'Manual'

                    self.outfile_prefix = self.MakeSafeFilename(self.descriptive_title)
                    self.number_of_exposures = int(mynxs.entry1.instrument.detector.count_time.size) 
                    self.exposure_time = float(mynxs.entry1.instrument.detector.count_time[0][0])
            except:
                self.logger.error('nxs file: '+str(self.nexus_file)+' could not be parsed')
                self.descriptive_title = 'ERROR IN NXS FILE'
                self.scan_type = 'Error'
                self.file_time = datetime.strftime(datetime.now(), '%Y/%m/%d_%H:%M:%S')
                self.visit_id = self.visit.ReturnVisitID()
        except Exception as ex:
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            self.logger.error(message)


    def MakeSafeFilename(self, descriptive_title):
        descriptive_title = re.sub('[-\s]+', '_', descriptive_title)
        descriptive_title = re.sub('[\.]+', 'p', descriptive_title)
        valid_chars = [chr(c) for c in range(ord('a'), ord('z')+1)+range(ord('A'), ord('Z')+1)+[ord('-'), ord('_')]+range(ord('0'), ord('9')+1)]
        descriptive_title = ''.join(c for c in descriptive_title if c in valid_chars)
        return descriptive_title

    def ReturnSQLDict(self, sqldict='nxs'):
        try:
            if sqldict == 'nxs':
                return {'nxs': [(self.nexus_file, self.scan_command, self.descriptive_title, self.scan_type, self.outfile_prefix, self.file_code, self.dat_dir, self.number_of_exposures, self.exposure_time, self.file_time, self.visit_id)]}

            else:
                datfile = sqldict
                not_parsed = True
                for dat_instance in self.dat_data:
                    if dat_instance.ReturnFilename() == datfile:
                        return {'raw_dat': [( datfile, self.ReturnFilename('raw_dat'), self.database.ReturnNextIndex(self.visit), self.nexus_file )]}
                        not_parsed = False
                if not_parsed:
                    raise IOError('No datfile called '+str(datfile))
        except Exception as ex:
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            self.logger.error(message)

    def ReturnFilename(self, dat_type='raw_dat'):
        try:
            if dat_type == 'raw_dat':
                return self.visit.ReturnVisitDirectory('raw_dat')+self.outfile_prefix+'_'+self.database.ReturnNextIndex(self.visit)+'.dat'
            elif dat_type == 'av_dat':
                first_index = self.database.ReturnNextIndex(self.visit)
                last_index = '{index:{fill}{align}{width}}'.format(index=int(first_index)+self.number_of_exposures, fill='0', align='>', width=5)
                return self.visit.ReturnVisitDirectory('av_dat')+self.outfile_prefix+'_'+first_index+'-'+last_index+'_av.dat'
            elif dat_type == 'sub_dat':
                first_index = self.database.ReturnNextIndex(self.visit)
                last_index = '{index:{fill}{align}{width}}'.format(index=int(first_index)+self.number_of_exposures, fill='0', align='>', width=5)
                return self.visit.ReturnVisitDirectory('sub_dat')+self.outfile_prefix+'_'+first_index+'-'+last_index+'_sub.dat'
            else:
                raise TypeError('nxs.ReturnFilename requires dat_type to be either raw_dat, av_dat or sub_dat')
        except Exception as ex:
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            self.logger.error(message)

    def ReturnDatFiles(self):
        dat_file_array = []
        if self.scan_command == 'static readout':
            for file_index in range(0,self.number_of_exposures):
                dat_file_array.append(self.dat_dir+self.myconfig['settings']['unsub_dir_pre']+str(self.file_code)+'_sample_'+str(file_index)+'.dat')
        return dat_file_array

    def WaitForDatFiles(self):
        once_through = False
        while (datetime.now() - datetime.strptime(self.file_time,'%Y/%m/%d_%H:%M:%S')).seconds < self.myconfig['settings']['time_out_time'] or not once_through:
            once_through = True
            all_present = True
            for file in self.ReturnDatFiles():
                if not os.path.isfile(file):
                    all_present = False
                    break
            if all_present:
                break
            time.sleep(self.myconfig['settings']['dwell_time'])
            
        return all_present

    def AddDatData(self, dat_instance):
        try:
            if dat_instance.type == 'raw_dat':
                dat_instance.output_name = self.ReturnFilename('raw_dat')
                dat_instance.index = self.database.ReturnNextIndex(self.visit)
                dat_instance.outname = self.ReturnFilename('raw_dat')
                dat_instance.outindex = self.database.ReturnNextIndex(self.visit)
                self.dat_data.append(dat_instance)
            else:
                raise TypeError('ParseNXS.AddDatData requires a RawDat instance as input')
        except Exception as ex:
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            self.logger.error(message)

    def ReturnDatInstances(self):
        return self.dat_data

if __name__ == '__main__':
    from visit_id import VisitID
    from database import Database
    visit = VisitID('sw14620-1')
    database = Database()
    robot_sample_file = '/dls/b21/data/2016/sw14620-1/b21-59103.nxs'
    instrument_scan_file = '/dls/b21/data/2016/cm14480-1/b21-43867.nxs'
    robot_buffer_file = '/dls/b21/data/2016/sw14620-1/b21-59102.nxs'
    manual_collection_file = '/dls/b21/data/2016/cm14480-1/b21-43521.nxs'
    sec_file = '/dls/b21/data/2016/cm14480-1/b21-45025.nxs'
    #job = ParseNXS(manual_collection_file)
    #job = ParseNXS(robot_sample_file)
    #job = ParseNXS(instrument_scan_file)
    job = ParseNXS(database, visit, robot_buffer_file)
    print job.ReturnFilename('raw_dat')
    print job.ReturnFilename('av_dat')
    print job.ReturnFilename('sub_dat')


        
