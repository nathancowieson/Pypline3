'''
Created on Mar 14th, 2016

@author: nathan
'''

import logging
import yaml
import os.path
from logging.handlers import TimedRotatingFileHandler

class myLogger(object):
    """Create a logger object for all the Pypline3 classes
    
    This class uses logging to create a single logger object for
    use by all the Pypline3 classes.
    """
    
    '''
    Constructor
    '''
    def __init__(self, calling_module):
        ###import config file
        pypline_dir = os.path.dirname(os.path.realpath(__file__))
        with open(pypline_dir+'/config.yaml', 'r') as ymlfile:
            self.myconfig = yaml.load(ymlfile)

        ###start a log file
        self.logger = logging.getLogger('Pypline3')
        self.logger.setLevel(logging.INFO)
        #formatter = logging.Formatter('%(asctime)s: %(levelname)s: %(module)s: %(message)s',"[%Y-%m-%d %H:%M:%S]")
        formatter = logging.Formatter('%(asctime)s: %(levelname)s: '+str(calling_module)+': %(message)s',"[%Y-%m-%d %H:%M:%S]")
        #the timed file handler will rotate to a new log file every 8 hours and keep 6 files i.e. 2 days
        filehandler = TimedRotatingFileHandler(self.myconfig['setup']['logfile'], when='H', interval=8, backupCount=6)
        filehandler.setFormatter(formatter)
        streamhandler = logging.StreamHandler()
        streamhandler.setFormatter(formatter)
        if len(self.logger.handlers) == 0:
            self.logger.addHandler(filehandler)
            self.logger.addHandler(streamhandler)
        self.logger.debug('Log file was reinitiated')

    def info(self, message='info log entry with no message'):
        self.logger.info(message)

    def debug(self, message='debug log entry with no message'):
        self.logger.debug(message)

    def error(self, message='error log entry with no message'):
        self.logger.error(message)

if __name__ == '__main__':
    job = myLogger()
    job.info('Testing the info logging function')
    job.error('Testing the error logging function')
