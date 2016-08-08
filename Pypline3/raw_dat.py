#!/home/b21user/nathanc/Pypline3/bin/python
'''
Created on Mar 16th, 2016

@author: nathan
'''
from logger import myLogger
import numpy
import os.path
import re
import yaml

class RawDat(object):
    """Parse a dat file
    
    The class takes as input the full path to a dat file and parses into a
    dictionary.
    """
    
    '''
    Constructor
    '''
    def __init__(self, datfile):
        ###import config file
        self.pypline_dir = os.path.dirname(os.path.realpath(__file__))
        with open(self.pypline_dir+'/config.yaml', 'r') as ymlfile:
            self.myconfig = yaml.load(ymlfile)

        ###connect to the logger
        self.logger = myLogger(os.path.basename(__file__))


        ###set some parameters
        self.type = 'raw_dat'
        self.dat_dict = {'Q': [], 'I': [], 'E': [], 'no_points': 0, 'low_q_window': 0.0, 'high_q_window': 0.0}
        self.datfile = datfile
        self.output_name = None
        self.index = None
        self.outindex = None
        self.outname = None
        ###parse on instantiation
        self.ParseDat(self.datfile)

    def ReturnColumn(self, column='Q'):
        if column in ['Q', 'I', 'E']:
            return self.dat_dict[column]
        else:
            self.logger.error('raw_dat.ReturnColumn needs either Q, I or E as an argument')

    def ParseDat(self, datfile):
        try:
            if not os.path.isfile(datfile) and str(datfile)[-4:] == '.dat':
                self.logger.error(datfile+' does not exist or is not of type dat')
                return False
            with open(datfile, 'r') as filedata:
                for line in filedata:
                    line = line.split()
                    try:
                        q = float(line[0])
                        i = float(line[1])
                        e = float(line[2])
                        if q > 0:
                            self.dat_dict['Q'].append(q)
                            self.dat_dict['I'].append(i)
                            self.dat_dict['E'].append(e)
                    except:
                        pass
            self.dat_dict['low_q_window'] = sum(self.dat_dict['I'][
                    int( round( self.myconfig['dat_data']['lowq_lo'] * len(self.dat_dict['I']) ) ):
                    int( round( self.myconfig['dat_data']['lowq_hi'] * len(self.dat_dict['I']) ) )
                    ] )
            self.dat_dict['high_q_window'] = sum(self.dat_dict['I'][
                    int( round( self.myconfig['dat_data']['highq_lo'] * len(self.dat_dict['I']) ) ):
                    int( round( self.myconfig['dat_data']['highq_hi'] * len(self.dat_dict['I']) ) )
                    ] )
            self.dat_dict['no_points'] = len(self.dat_dict['Q'])
            return True
        except Exception as ex:
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            self.logger.error(message)
            return False        
    
    def ReturnFilename(self):
        return self.datfile

    def ReturnDatDict(self):
        return self.dat_dict

    def OutputDatDictForFile(self, outfile_name=None):
        try:
            dat_dict = self.dat_dict
            if outfile_name == None:
                if self.output_name == None:
                    raise ValueError('OutputDatDictForFile needs an output name')
                else:
                    outfile_name = self.output_name
            dat_dict['output_name'] = outfile_name
            dat_dict['headers'] = ['# Diamond Light Source Ltd.','# Data extracted from: '+str(self.ReturnFilename())]    
            return dat_dict
        except Exception as ex:
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            self.logger.error(message)
            return False        
        
        

if __name__ == '__main__':
    good_dat_file = '/dls/b21/data/2016/cm14480-1/processed/b21-43884.unsub/b21-43884_sample_0.dat'
    bad_dat_file = '/dls/b21/data/2016/cm14480-1/processed/b21-41893.unsub/b21-41893_nosample_0.dat'
    job = RawDat(good_dat_file)
    print job.ReturnFilename()
    print 'finished cleanly'


         
