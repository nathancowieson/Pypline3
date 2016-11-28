#!/home/b21user/nathanc/Pypline3/bin/python
'''
Created on Mar 15th, 2016

@author: nathan
'''
from datetime import datetime
from epics import ca
from logger import myLogger
import os.path
import yaml

class VisitID(object):
    """Returns the visit id
    
    This class contains functions to return the visit ID, either
    retrieving the current ID from epics or returning an ID that
    has been set by the user.
    """
    
    '''
    Constructor
    '''
    def __init__(self, visit_id = None):
        ###import config file
        self.pypline_dir = os.path.dirname(os.path.realpath(__file__))
        with open(self.pypline_dir+'/config.yaml', 'r') as ymlfile:
            self.myconfig = yaml.load(ymlfile)

        ###connect to the logger
        self.logger = myLogger(os.path.basename(__file__))


        ###set some parameters
        self.chid = ca.create_channel(self.myconfig['settings']['visit_id_pv'], connect=True)
        self.visit_id = visit_id
        self.type = 'visit'

    def ReturnVisitID(self):
        try:
            if self.visit_id == None:
                if ca.isConnected(self.chid):
                    return ca.get(self.chid)
                else:
                    raise EnvironmentError('Cannot connect to epics')
            else:
                return self.visit_id

        except Exception as ex:
            self.conn.rollback()
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            self.logger.error(message)

    def ReturnYear(self):
        try:
            for year in reversed(range(datetime.now().year - 5, datetime.now().year + 1)):
                if os.path.isdir(self.myconfig['setup']['poll_dir']+str(year)+'/'+str(self.ReturnVisitID())):
                    return year
            raise EnvironmentError('Visit ID does not exist in the file system')
        except Exception as ex:
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            self.logger.error(message)
            return False        

    def ReturnDatabaseFileName(self):
        if self.ReturnYear() == False:
            return False
        else:
            return str(self.myconfig['setup']['poll_dir']+str(self.ReturnYear())+'/'+str(self.ReturnVisitID()))+self.myconfig['settings']['dat_output_dir']+str(self.ReturnVisitID())+'.db'
        
    def ReturnVisitDirectory(self, type=None):
        if self.ReturnYear() == False:
            return False
        else:
            visit_dir = str(self.myconfig['setup']['poll_dir']+str(self.ReturnYear())+'/'+str(self.ReturnVisitID()))
            if type == 'raw_dat':
                return str(self.myconfig['setup']['poll_dir']+str(self.ReturnYear())+'/'+str(self.ReturnVisitID()))+self.myconfig['settings']['dat_output_dir']+self.myconfig['settings']['raw_dat_dir']
            elif type == 'av_dat':
                return str(self.myconfig['setup']['poll_dir']+str(self.ReturnYear())+'/'+str(self.ReturnVisitID()))+self.myconfig['settings']['dat_output_dir']+self.myconfig['settings']['av_dat_dir']
            elif type == 'sub_dat':
                return str(self.myconfig['setup']['poll_dir']+str(self.ReturnYear())+'/'+str(self.ReturnVisitID()))+self.myconfig['settings']['dat_output_dir']+self.myconfig['settings']['sub_dat_dir']
            else:
                return visit_dir

    def ReturnSQLDict(self):
        if self.ReturnYear() == False:
            return False
        else:
            return {'visit': [(str(self.ReturnVisitID()), str(self.myconfig['setup']['poll_dir']+str(self.ReturnYear())+'/'+str(self.ReturnVisitID())), int(self.ReturnYear()))]}

    def SetVisitID(self, visit_id):
        self.visit_id = visit_id

    def MakeOutputDirs(self):
        try:
            output_dir = self.ReturnVisitDirectory()+self.myconfig['settings']['dat_output_dir']
            if os.path.isdir(self.ReturnVisitDirectory()):
                for mydir in ['', self.myconfig['settings']['raw_dat_dir'], self.myconfig['settings']['av_dat_dir'], self.myconfig['settings']['sub_dat_dir']]:
                    if not self.MakeDirectory(output_dir+mydir):
                        raise IOError('Could not make the directory: '+output_dir+mydir)
            else:
                raise IOError('The visit ID: '+self.ReturnVisitID()+' has no directory')
        except Exception as ex:
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            self.logger.error(message)

    def DirectoryExists(self,directory):
        it_exists = False
        try:
            if os.path.isdir(directory):
                it_exists = True
            return it_exists
        except:
            self.logger.error('VisitID.DirectoriesExist failed to test properly')
            return it_exists

    def MakeDirectory(self, directory):
        try:
            if not self.DirectoryExists(directory):
                self.logger.info('Making directory: '+directory)
                os.mkdir(directory)
            else:
                self.logger.debug('Directory already exists: '+directory)
            return True
        except Exception as ex:
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            self.logger.error(message)
            return False

if __name__ == '__main__':
    job = VisitID()
    print 'current visit: '+str(job.ReturnVisitID())
    print job.ReturnDatabaseFileName()


        
