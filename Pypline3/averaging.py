#!/home/b21user/nathanc/Pypline3/bin/python
'''
Created on Mar 16th, 2016

@author: nathan
'''
from logger import myLogger
import numpy
import os.path
import re
from scipy.stats import ttest_ind
import sys
import yaml

class Averaging(object):
    """Add ParseNXS objects and make an object for averaging
    
    The class is instantiated without any arguments and ParseNXS objects
    are added as the come in. All of the dat files within each ParseNXS
    object will be averaged together as though they were one big file.
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
        self.sql_dict = { 'averaging_instance': [], 'av_dat': [] }
        self.dat_dict = {'Q': [], 'I': [], 'E': [], 'headers': [], 'output_name': None}
        self.dat_dict['headers'].append('# Diamond Light Source Ltd.')
        self.dat_dict['headers'].append('# Averaged/unblanked data produced by the B21 automated Pypline')
        self.lo_qs = []
        self.hi_qs = []
        self.parsednxs_list = []
        self.dat_instances = []
        self.intensities_dict = {}
        self.is_good = []
        self.errors_dict = {}
        self.indeces = []
        self.type = 'averaging'

    def LimitToWindowSize(self, window_size=0):
        try:
            if window_size == 0:
                window_size = int(self.myconfig['dat_data']['sec_buffer_window_size'])
            else:
                window_size = int(window_size)
            limited = False
            if not type(window_size) == type(1):
                raise TypeError('averaging:LimitToWindowSize needs an integer')
            arrays_to_limit = [self.lo_qs, self.hi_qs, self.dat_instances, self.is_good, self.indeces]
            for q in self.intensities_dict.keys():
                arrays_to_limit.append(self.intensities_dict[q])
                arrays_to_limit.append(self.errors_dict[q])
            if len(arrays_to_limit[0]) == len(self.parsednxs_list):
                arrays_to_limit.append(self.parsednxs_list)
            for my_array in arrays_to_limit:
                while len(my_array) > window_size:
                    limited = True
                    del my_array[0]
            if limited:
                self.logger.info('limited the buffer array to the last '+str(window_size)+' shots')

        except Exception as ex:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            line_no = exc_tb.tb_lineno
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            message = fname+', line: '+str(line_no)+': '+message
            self.logger.error(message)
            return False        

    def TestSimilar(self, intensities_array):
        try:
            if not type(intensities_array) == type([]):
                raise TypeError('TestSimilar requires an array of intensity values')
            if len(self.intensities_dict.keys()) == 0:
                return 1
            if not len(self.intensities_dict.keys()) == len(intensities_array):
                raise IndexError('averaging:TestSimilar had different q values in the test file compared to the averaged set')
            reference = self.ReturnMedianI()
            ttest = ttest_ind(intensities_array, reference)
            if ttest[1] <= 0.96:
                self.logger.info('repeat sec buffer seems to be different')
                return 0
            else:
                return 1

        except Exception as ex:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            line_no = exc_tb.tb_lineno
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            message = fname+', line: '+str(line_no)+': '+message
            self.logger.error(message)
            return 0

    def ReturnMedianI(self):
        medianI = []
        for q in sorted(self.intensities_dict.keys()):
            medianI.append(numpy.median(self.intensities_dict[q]))
        return medianI
        
    def AddParsedNXS(self, parsednxs):
        try:
            if not parsednxs.type == 'nxs':
                raise TypeError('Averaging.AddParsedNXS needs a ParsedNXS object')
            self.parsednxs_list.append(parsednxs)
            self.logger.info('Adding a parsednxs object to averaging class with '+str(len(parsednxs.ReturnDatInstances()))+' dat objects')
            for dat_object in parsednxs.ReturnDatInstances():
                self.dat_instances.append(dat_object)
                #self.is_good.append(self.TestSimilar(dat_object.ReturnColumn('I')))
                self.is_good.append(dat_object.ReturnColumn('I'))
                self.lo_qs.append(dat_object.dat_dict['low_q_window'])
                self.hi_qs.append(dat_object.dat_dict['high_q_window'])
                self.indeces.append(dat_object.index)
                for index, q in enumerate(dat_object.dat_dict['Q']):
                    if q in self.intensities_dict.keys():
                        self.intensities_dict[q].append(dat_object.dat_dict['I'][index])
                    else:
                        self.intensities_dict[q] = [ dat_object.dat_dict['I'][index] ]
                    if q in self.errors_dict.keys():
                        self.errors_dict[q].append(dat_object.dat_dict['E'][index])
                    else:
                        self.errors_dict[q] = [ dat_object.dat_dict['E'][index] ]

        except Exception as ex:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            line_no = exc_tb.tb_lineno
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            message = fname+', line: '+str(line_no)+': '+message
            self.logger.error(message)
            return False

    def RejectOutliers(self):
        try:
            if not len(self.lo_qs) == len(self.hi_qs) == len(self.is_good):
                raise IndexError('averaging had a problem importing the dat data')
            if len(self.hi_qs) == 0:
                raise IndexError('parsednxs object has no dat instances')
                       
            #RESET ALL DAT FILES TO BAD
            self.sql_dict['averaging_instance'] = []
            for index, status in enumerate(self.is_good):
                if not status == 0:
                    self.is_good[index] = 0
            #IF HIGH Q IS ABOVE THRESHOLD MARK AS GOOD, GETS RID OF AIR SHOTS
            high_q_target = max(self.hi_qs)
            for index, hi_q in enumerate(self.hi_qs):
                if hi_q >= high_q_target * self.myconfig['dat_data']['highq_deadband'] :
                    self.is_good[index] = 1
                    #SET LOW Q TARGET ONLY USING THE NON-AIR SHOTS
                    try:
                        if self.lo_qs[index] < low_q_target:
                            low_q_target = self.lo_qs[index]
                    except:
                        low_q_target = self.lo_qs[index]
            #SET INDEX TO BAD FOR GOOD SHOTS THAT FAIL LOW Q TEST, GETS RID OF AGGREGATION
            for index, item in enumerate(self.is_good):
                if item == 1:
                    if not self.lo_qs[index] <= low_q_target * self.myconfig['dat_data']['lowq_deadband']:
                        self.is_good[index] = 0

        except Exception as ex:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            line_no = exc_tb.tb_lineno
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            message = fname+', line: '+str(line_no)+': '+message
            self.logger.error(message)
            return False

    def FlushDatDict(self):
        self.dat_dict = {'Q': [], 'I': [], 'E': [], 'headers': [], 'output_name': None}

    def Average(self):
        try:
            self.FlushDatDict()
            if len(self.dat_instances) < 1:
                raise IndexError('Averaging tried to average with no dat files in it')
            output_prefix = re.split('\d{'+str(self.myconfig['settings']['number_digits_in_output_index'])+'}-\d{'+str(self.myconfig['settings']['number_digits_in_output_index'])+'}', self.parsednxs_list[0].ReturnFilename('av_dat'))
            indeces = str(min(self.indeces))+'-'+str(max(self.indeces))
            used_files = []
            self.dat_dict['output_name'] = output_prefix[0]+indeces+output_prefix[-1]

            self.RejectOutliers()
            self.sql_dict['averaging_instance'] = []
            for index, dat_instance in enumerate(self.dat_instances):
                self.sql_dict['averaging_instance'].append( (None, self.is_good[index], dat_instance.ReturnFilename(), self.dat_dict['output_name']) )
                if self.is_good[index] == 1:
                    used_files.append(os.path.split(dat_instance.outname)[-1])
            self.dat_dict['headers'].append('#'+','.join(used_files))
            self.sql_dict['av_dat'] = [ ( self.dat_dict['output_name'], len(self.is_good), len(self.is_good)/float(sum(self.is_good)) ) ]

            for q in sorted(self.intensities_dict.keys()):
                self.dat_dict['Q'].append(q)
                self.dat_dict['I'].append(sum([ i * self.is_good[index] for index, i in enumerate(self.intensities_dict[q]) ]) / sum(self.is_good))
                self.dat_dict['E'].append(numpy.sqrt( sum( [numpy.power(e * self.is_good[index], 2) for index, e in enumerate(self.errors_dict[q])] ) / sum(self.is_good) ))
            return ( self.dat_dict, self.sql_dict )

            

        except Exception as ex:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            line_no = exc_tb.tb_lineno
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            message = fname+', line: '+str(line_no)+': '+message
            self.logger.error(message)
            return ({}, {})
                    
                
        
if __name__ == '__main__':
    from raw_dat import RawDat
    from database import Database
    from visit_id import VisitID
    from nxs import ParseNXS
    visit = VisitID('sm15568-1')
    database = Database()
    averaging = Averaging()
    good_buffer = '/dls/b21/data/2016/sm15568-1/b21-140945.nxs'
    air_buffer = '/dls/b21/data/2016/sm15568-1/b21-140918.nxs'
    good_sample = '/dls/b21/data/2016/sm15568-1/b21-140923.nxs'
    rad_damage_sample = '/dls/b21/data/2016/sm15568-1/b21-140919.nxs'

    #parsednxs = ParseNXS(database, visit, good_buffer)
    #for datfile in parsednxs.ReturnDatFiles():
    #    parsednxs.AddDatData(RawDat(datfile))
    #averaging.AddParsedNXS(parsednxs)
    #averaging.RejectOutliers()
    #print 'this should be all ones:'
    #print averaging.is_good

    #parsednxs = ParseNXS(database, visit, air_buffer)
    #for datfile in parsednxs.ReturnDatFiles():
    #    parsednxs.AddDatData(RawDat(datfile))
    #averaging.AddParsedNXS(parsednxs)
    #averaging.RejectOutliers()
    #print 'this should be first half ones, second half zeros:'
    #print averaging.is_good

    parsednxs = ParseNXS(database, visit, rad_damage_sample)
    for datfile in parsednxs.ReturnDatFiles():
        parsednxs.AddDatData(RawDat(datfile))
    averaging.AddParsedNXS(parsednxs)
    averaging.RejectOutliers()
    print 'this should be six ones then all zeros:'
    print averaging.is_good

    #This should make an averaged file in the directory /dls/b21/data/2016/sm15568-1/processing/pypline/av_dat/
    visit.MakeOutputDirs()
    myaverage = averaging.Average()
    database.SaveDatFile(myaverage[0])

         
