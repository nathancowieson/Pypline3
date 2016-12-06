#!/home/b21user/nathanc/Pypline3/bin/python
'''
Created on Mar 16th, 2016

@author: nathan
'''
from logger import myLogger
import os.path
import re
import yaml
from zipfile import ZipFile

class Zippy(object):
    """Create a zip archive and add files to it
    
    The class takes a parsednxs object with dat files and adds the dat files
    to a zip archive.
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
        self.type = 'zippy'
        self.first_commit = True
        self.zip_file_name = None
        self.parsednxs = None

    def Reset(self):
        self.parsednxs = None
        self.zip_file_name = None
        self.first_commit = True

    def AddParsednxs(self, parsednxs):
        try:
            if not parsednxs.type == 'nxs':
                raise AttributeError('Added parsednxs object was not of the right type')
            elif len(parsednxs.dat_data) < 1:
                raise AttributeError('Added parsednxs object has no dat files')
            else:
                if self.first_commit:#first nxs file added creates the zip file name
                    self.parsednxs = parsednxs
                    self.SetZipName(self.MakeZipFileName())
                for datobj in parsednxs.dat_data:
                    self.AddFile(datobj.output_name)#first dat file committed creates the zip file and sets self.first commit to False, subsequent additions append to this

        except Exception as ex:
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            self.logger.error(message)
        
    def SetZipName(self, zip_file_name):
        if os.path.exists(os.path.split(os.path.abspath(zip_file_name))[0]):
            if zip_file_name[-4:] == '.zip':
                self.zip_file_name = zip_file_name
            else:
                self.logger.error('Zip file name must end in .zip')
        else:
            self.logger.error('Specified directory does not exist')

    def ReturnZipName(self):
        return self.zip_file_name

    def MakeZipFileName(self):
        try:
            if self.parsednxs == None:
                raise AttributeError('Cannot make a zip file output name without any parsednxs file')
            if len(self.parsednxs.dat_data) > 0:
                index = 1
                while True:
                    zip_outfile = self.parsednxs.visit.ReturnVisitDirectory('zip_dat')+self.parsednxs.outfile_prefix+'_'+str(index)+'.zip'
                    if os.path.isfile(zip_outfile):
                        index += 1
                    else:
                        break
                return zip_outfile
            else:
                raise AttributeError('Cannot make a zip file output name without any dat files in the parsednxs file')
                return False
        except Exception as ex:
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            self.logger.error(message)

    def AddFile(self, filename):
        try:
            if self.first_commit:
                self.logger.info('Creating a new zip file: '+self.zip_file_name)
                with ZipFile(self.zip_file_name, 'w') as myzip:
                    myzip.write(filename)
                self.first_commit = False
            else:
                self.logger.info('Appending dat file to zip file')
                with ZipFile(self.zip_file_name, 'a') as myzip:
                    myzip.write(filename, os.path.basename(filename))
        except:
            self.logger.error('Could not add file: '+filename)


if __name__ == '__main__':
    from raw_dat import RawDat
    from nxs import ParseNXS
    from visit_id import VisitID
    from database import Database
    
    files = ['/dls/b21/data/2016/cm14480-1/b21-61782.nxs','/dls/b21/data/2016/cm14480-1/b21-61783.nxs','/dls/b21/data/2016/cm14480-1/b21-61784.nxs','/dls/b21/data/2016/cm14480-1/b21-61785.nxs','/dls/b21/data/2016/cm14480-1/b21-61786.nxs','/dls/b21/data/2016/cm14480-1/b21-61787.nxs']
    visit = VisitID('cm14480-1')
    database = Database()
    database.setDatabase(visit.ReturnDatabaseFileName())
    job = Zippy()
    if len(database.getTables()) == 0:
        database.createDatabase()
    for nxsfile in files:
        parsednxs = ParseNXS(database, visit, nxsfile)
        for datfile in parsednxs.ReturnDatFiles():
            parsednxs.AddDatData(RawDat(datfile))
            job.AddParsednxs(parsednxs)
            print job.ReturnZipName()
    job.logger.info('FINISHED NORMALLY')




         
