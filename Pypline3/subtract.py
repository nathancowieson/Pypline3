#!/home/b21user/nathanc/Pypline3/bin/python
'''
Created on Aug 24th, 2016

@author: nathan
'''
from glob import glob
from logger import myLogger
import numpy
import os.path
import re
from scipy.stats import ttest_ind
import subprocess
import sys
import time
import yaml

class Subtraction(object):
    """Add Averaging objects for buffer and sample and do a subtraction
    
    The class is instantiated without any arguments and Averaging objects
    are added for buffer and sample. Only one buffer and one sample object
    will be held at any one time and addition of a second will overwrite
    any previous.
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
        self.sql_dict = { 'sub_dat': [] }
        self.dat_dict = {'Q': [], 'I': [], 'E': [], 'headers': [], 'output_name': None}
        self.dat_dict['headers'].append('# Diamond Light Source Ltd.')
        self.dat_dict['headers'].append('# Blanked data produced by the B21 automated Pypline')
        self.highq_signal = 0.0
        self.rg = 0.0
        self.i_zero = 0.0
        self.volume = 0.0
        self.mass = 0.0
        self.sample = None
        self.buffer = None
        self.type = 'subtraction'
        self.autorg_data = {}

    def AddSample(self, sample=None):
        try:
            if sample.type == 'averaging':
                self.sample = sample
                self.logger.info('Subtraction object received an Averaging instance for sample')
            else:
                raise TypeError('Subtraction/AddSample requires an object of type averaging')

        except Exception as ex:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            line_no = exc_tb.tb_lineno
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            message = fname+', line: '+str(line_no)+': '+message
            self.logger.error(message)
            return False        

    def AddBuffer(self, buffer=None):
        try:
            if buffer.type == 'averaging':
                self.buffer = buffer
                self.logger.info('Subtraction object received an Averaging instance for buffer')
            else:
                raise TypeError('Subtraction/AddBuffer requires an object of type averaging')

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

    def Subtract(self):
        try:
            if len(self.sample.dat_dict['Q']) < 1 or len(self.buffer.dat_dict['Q']) < 1:
                raise IndexError('Subtraction tried to subtract two averaging objects with no dat data')
            elif not self.sample.dat_dict['Q'] == self.buffer.dat_dict['Q']:
                raise IndexError('Subtraction tried to subtract two averaging objects with different Q ranges')
            else:
                self.FlushDatDict()
                for index, q in enumerate(self.buffer.dat_dict['Q']):
                    self.dat_dict['Q'].append(q)
                    self.dat_dict['I'].append( self.sample.dat_dict['I'][index] - self.buffer.dat_dict['I'][index] )
                    self.dat_dict['E'].append(numpy.sqrt( numpy.power(self.sample.dat_dict['E'][index], 2) + numpy.power(self.buffer.dat_dict['E'][index], 2) ))

            self.GenerateStats()

            #Collate data for database etc.
            output_prefix = re.split('\d{'+str(self.myconfig['settings']['number_digits_in_output_index'])+'}-\d{'+str(self.myconfig['settings']['number_digits_in_output_index'])+'}', self.sample.parsednxs_list[0].ReturnFilename('sub_dat'))
            indeces = str(min(self.sample.indeces))+'-'+str(max(self.sample.indeces))
            self.dat_dict['output_name'] = output_prefix[0]+indeces+output_prefix[-1]
            self.dat_dict['headers'].append('#Sample file: '+self.sample.dat_dict['output_name'])
            self.dat_dict['headers'].append('#Buffer file: '+self.buffer.dat_dict['output_name'])

            self.sql_dict['sub_dat'].append( (self.dat_dict['output_name'], self.sample.dat_dict['output_name'], self.buffer.dat_dict['output_name'], self.highq_signal, self.rg, self.i_zero, self.volume, self.mass) )
            return( self.dat_dict, self.sql_dict )

        except Exception as ex:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            line_no = exc_tb.tb_lineno
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            message = fname+', line: '+str(line_no)+': '+message
            self.logger.error(message)
            return ({}, {})

    def WriteDatFile(self, outfile_name='/tmp/temp.dat'):
        self.logger.info('Writing a temporary dat file for stats generation')
        try:
            if not len(self.dat_dict['Q']) > 0:
                raise IndexError('subtract:WriteDatFile cannot run with no dat data')
            outstring_array = []
            for index, q in enumerate(self.dat_dict['Q']):
                outstring_array.append('{0:<16.10f}{1:<16.10f}{2:<16.10f}'.format(
                        q,
                        self.dat_dict['I'][index],
                        self.dat_dict['E'][index]))

            outfile = open(outfile_name, 'w')
            outfile.write('\n'.join(outstring_array))
            outfile.close()
            return True
        except Exception as ex:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            line_no = exc_tb.tb_lineno
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            message = fname+', line: '+str(line_no)+': '+message
            self.logger.error(message)
            return False

    def CalculateHighQSignal(self):
        self.logger.info('Calculating the signal strength at high Q')
        try:
            if not len(self.dat_dict['Q']) > 0:
                raise IndexError('subtract:CalculateHighQSignal cannot run with no dat data')
            #Calculate high_q signal
            try:
                start_highq = int(round( len(self.dat_dict['Q']) * self.myconfig['dat_data']['highq_lo'] ))
                end_highq = int(round( len(self.dat_dict['Q']) * self.myconfig['dat_data']['highq_hi'] ))
                highq_window = numpy.array(self.dat_dict['I'][start_highq:end_highq])
                return float(highq_window.mean())
            except:
                raise IndexError('Could not calculate high Q signal')
        except Exception as ex:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            line_no = exc_tb.tb_lineno
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            message = fname+', line: '+str(line_no)+': '+message
            self.logger.error(message)
            return 0.0

    def RunAutoRg(self, infile_name='/tmp/temp.dat'):
        self.logger.info('Running autorg')
        autorg_executable = 'autorg'
        try:
            #Run autorg
            if not os.path.isfile(infile_name):
                time.sleep(1)
                if not os.path.isfile(infile_name):
                    raise SystemError('Cannot run autorg, '+infile_name+' does not exist')

            try:
                my_env = os.environ.copy()
                atsas = self.myconfig['setup']['atsas_dir']
                my_env["ATSAS"] = atsas
                my_env["PATH"] = atsas + '/bin:' + my_env["PATH"]
                my_env["LD_LIBRARY_PATH"] = atsas + '/lib64/atsas'
                child = subprocess.Popen([autorg_executable, '-f', 'csv', infile_name], env=my_env, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
                autorg_output = child.communicate()[0].split('\n')
                autorg_status = child.returncode
            except:
                self.logger.info('Could not run autorg, signal too low')
                autorg_output = []
                return ( 0.0, 0.0 )

            if len(autorg_output) > 1:
                headers = autorg_output[0].split(',')

                items = autorg_output[1].split(',')
                if len(headers) == len(items):
                    for index, item in enumerate(headers):
                        try:
                            self.autorg_data[item] = float(items[index])
                        except:
                            self.autorg_data[item] = items[index]
                    try:
                        return ( self.autorg_data['Rg'], self.autorg_data['I(0)'] )
                    except:
                        raise KeyError('Rg and I(0) were not in the autorg output')
                else:
                    raise IndexError('Could not parse autorg results')
            else:
                raise IndexError('Could not  parse autorg results')
            
        except Exception as ex:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            line_no = exc_tb.tb_lineno
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            message = fname+', line: '+str(line_no)+': '+message
            self.logger.error(message)
            return ( 0.0, 0.0 )

    def RunGnom(self, infile_name='/tmp/temp.dat', outfile_name='temp.out'):
        self.logger.info('Running datgnom and datporod')
        datgnom_executable = 'datgnom'
        datporod_executable = 'datporod'

        try:            
            #Run datgnom
            if not os.path.isfile(infile_name):
                time.sleep(1)
                if not os.path.isfile(infile_name):
                    raise SystemError('Cannot run datgnom, '+infile_name+' does not exist')

            command = [datgnom_executable]
            if 'First point' in self.autorg_data.keys():
                command.append('-s')
                command.append(int(self.autorg_data['First point']))

            if 'Rg' in self.autorg_data.keys():
                command.append('-r')
                command.append(self.autorg_data['Rg'])

            command.append('-o')
            command.append(outfile_name)
            command.append(infile_name)
            try:
                my_env = os.environ.copy()
                atsas = self.myconfig['setup']['atsas_dir']
                my_env["ATSAS"] = atsas
                my_env["PATH"] = atsas + '/bin:' + my_env["PATH"]
                my_env["LD_LIBRARY_PATH"] = atsas + '/lib64/atsas'
                child = subprocess.Popen([str(x) for x in command], env=my_env, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
                datgnom_output = child.communicate()[0].split('\n')
                datgnom_status = child.returncode
            except:
                self.logger.info('datgnom failed to run, probably too low signal')
                datgnom_status = 1

            if datgnom_status == 0:
                #Run datporod
                command = [datporod_executable, outfile_name]
                try:
                    my_env = os.environ.copy()
                    atsas = self.myconfig['setup']['atsas_dir']
                    my_env["ATSAS"] = atsas
                    my_env["PATH"] = atsas + '/bin:' + my_env["PATH"]
                    my_env["LD_LIBRARY_PATH"] = atsas + '/lib64/atsas'
                    child = subprocess.Popen(command, env=my_env, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
                    datporod_output = child.communicate()[0].split()
                    datporod_status = child.returncode
                except:
                    self.logger.error('Datporod did not run cleanly for some reason')
                    return 0.0

            if len(datporod_output) == 4 and datporod_output[-1] == outfile_name:
                return float(datporod_output[-2])
            else:
                raise IndexError('Datporod output is not standard and could not be processed')
            

        except Exception as ex:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            line_no = exc_tb.tb_lineno
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            message = fname+', line: '+str(line_no)+': '+message
            self.logger.error(message)
            return 0.0

    def CalculateMass(self, volume=None):
        if volume == None:
            volume = self.volume
        try:
            try:
                volume = float(volume)
            except:
                raise TypeError('subtract:CalculateMass requires a volume of type float')
            return round(volume / 1700, 1)

        except Exception as ex:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            line_no = exc_tb.tb_lineno
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            message = fname+', line: '+str(line_no)+': '+message
            self.logger.error(message)
            return 0.0
        

    def GenerateStats(self):
        self.logger.info('Generating stats')
        home_directory = os.getcwd()
        os.chdir('/tmp')

        filelist_before = set(glob('*'))
        self.highq_signal = self.CalculateHighQSignal()
        if self.WriteDatFile():
            self.rg, self.i_zero = self.RunAutoRg()
            self.volume = self.RunGnom()
            self.mass = self.CalculateMass()
        else:
            self.logger.error('subtract:GenerateStats failed to output a dat file')
        filelist_after = set(glob('*'))
        temp_files = list(filelist_after - filelist_before)
        self.logger.info('Removing '+str(len(temp_files))+' temporary files')
        for temp_file in temp_files:
            os.remove(temp_file)
        os.chdir(home_directory)

if __name__ == '__main__':
    from raw_dat import RawDat
    from database import Database
    from visit_id import VisitID
    from nxs import ParseNXS
    from averaging import Averaging
    visit = VisitID('sm15568-1')
    database = Database()
    good_buffer = '/dls/b21/data/2016/sm15568-1/b21-140945.nxs'
    my_buffer = Averaging()
    parsednxs = ParseNXS(database, visit, good_buffer)
    database.insertData(parsednxs.ReturnSQLDict('nxs'))
    for datfile in parsednxs.ReturnDatFiles():
        parsednxs.AddDatData(RawDat(datfile))
        database.SaveDatFile(parsednxs.dat_data[-1].OutputDatDictForFile())
        database.insertData(parsednxs.ReturnSQLDict(datfile))
    my_buffer.AddParsedNXS(parsednxs)
    my_buffer.Average()

    good_sample = '/dls/b21/data/2016/sm15568-1/b21-140923.nxs'
    my_sample = Averaging()
    parsednxs = ParseNXS(database, visit, good_sample)
    database.insertData(parsednxs.ReturnSQLDict('nxs'))
    for datfile in parsednxs.ReturnDatFiles():
        parsednxs.AddDatData(RawDat(datfile))
        database.SaveDatFile(parsednxs.dat_data[-1].OutputDatDictForFile())
        database.insertData(parsednxs.ReturnSQLDict(datfile))
    my_sample.AddParsedNXS(parsednxs)
    my_sample.Average()

    subtract = Subtraction()
    subtract.AddSample(my_sample)
    subtract.AddBuffer(my_buffer)
    subtract.Subtract()
    subtract.GenerateStats()
    #visit.MakeOutputDirs()
    #myaverage = averaging.Average()
    #database.SaveDatFile(myaverage[0])
