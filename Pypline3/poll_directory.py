'''
Created on Mar 17th, 2016

@author: nathan
'''
import glob
from logger import myLogger
import os.path
import re
import yaml

class PollFileSystem(object):
    """Polls a given experiment folder for new nexus files
    
    Given a visit_id object will poll a directory in the
    file system and return a list of all nexus files.
    """
    '''
    Constructor
    '''
    def __init__(self, visit_id_object):
        ###import config file
        self.pypline_dir = os.path.dirname(os.path.realpath(__file__))
        with open(self.pypline_dir+'/config.yaml', 'r') as ymlfile:
            self.myconfig = yaml.load(ymlfile)

        ###connect to the logger
        self.logger = myLogger(os.path.basename(__file__))

        self.visit_id_object = visit_id_object

    def ReturnNxsFiles(self):
        try:
            return glob.glob(self.visit_id_object.ReturnVisitDirectory()+'/*.nxs')
        except:
            self.logger.error('The visit specified does not have a matching directory')
            return []

if __name__ == '__main__':
    from visit_id import VisitID
    job = PollFileSystem(VisitID())
    print job.ReturnNxsFiles()

        
