#!/dls/science/groups/b21/PYTHON/bin/python
import os.path
import yaml
import sys
from visit_id import VisitID
from poll_directory import PollFileSystem
from database import Database
from logger import myLogger
from nxs import ParseNXS
from raw_dat import RawDat
from dat_manager import DatManager
from averaging import Averaging
from subtract import Subtraction
from zippy import Zippy
from optparse import OptionParser
from optparse import OptionGroup
import time


#START A LOG FILE
log = myLogger(os.path.basename(__file__))

#PARSE THE COMMAND LINE OPTIONS
parser = OptionParser()
optional = OptionGroup(parser, "Optional Arguments")
optional.add_option("-v", "--visit_id", action="store", type="string", dest="visit_id", default="None", help="The visit ID of the experiment i.e. mx9537-84. If none is given will attempt to retrieve visit ID from database.")
optional.add_option("-p", "--poll", action="store_true", dest="poll", default=False, help="All current nxs files are parsed and the program will terminate. Including the switch with no arguments will cause the script to finish current files and stay active waiting for new files to be added.")

parser.add_option_group(optional)
(options, args) = parser.parse_args()

#GET THE CONFIG FILE
pypline_dir = os.path.dirname(os.path.realpath(__file__))
with open(pypline_dir+'/config.yaml', 'r') as ymlfile:
    myconfig = yaml.load(ymlfile)

#SET SOME PARAMETERS
buffers = Averaging()
samples = []

#CONNECT TO THE SQLITE DATABASE
database = Database()

#AN OBJECT TO HANDLE AVERAGING AND SUBTRACTING
dat_manager = DatManager()

polling = 1
while polling:
    #IF POLLING IS NOT SPECIFIED THEN RUN THROUGH ONCE AND STOP
    if not options.poll:
        log.info('Not polling, will run through once and stop')
        polling = 0

    #DEFINE THE VISIT
    if options.visit_id == 'None':
        visit = VisitID()
        log.info('Visit automatically set to '+str(visit.ReturnVisitID()))
    else:
        visit = VisitID(options.visit_id)
        log.info('Visit ID was set manually to '+str(visit.ReturnVisitID()))
    visit.MakeOutputDirs()

    #CHECK AND/OR CREATE DATABASE FILE FOR THIS VISIT
    database.setDatabase(visit.ReturnDatabaseFileName())
    if len(database.getTables()) == 0:
        database.createDatabase()

    #A VARIABLE FOR MAKING DECISIONS ABOUT ROBOT BUFFERS
    previous_scan_type = None
    
    #MAKE A VISIT ENTRY IN THE DATABASE IF ITS NOT THERE ALREADY
    if not visit.ReturnVisitID() in database.ReturnVisits():
        log.info('Entered visit: '+visit.ReturnVisitID()+' to the database')
        database.insertData(visit.ReturnSQLDict())#database the visit
    
    #CONNECT TO THE FILE SYSTEM
    files = PollFileSystem(visit)

    #START AN OBJECT FOR MANAGING THE ZIP DIRECTORY OUTPUT
    zippy = Zippy()

    #PARSE THE NEW NXS FILES
    nxsfiles = sorted( list( set(files.ReturnNxsFiles()) - set(database.ReturnVisitNxsFiles(visit)) ) )
    for nxsfile in nxsfiles:
        parsednxs = ParseNXS(database, visit, nxsfile)
        log.info('------------NEW NXS: '+str(nxsfile)+'------------')
        database.insertData(parsednxs.ReturnSQLDict('nxs'))
        if parsednxs.WaitForDatFiles():
            for datfile in parsednxs.ReturnDatFiles():
                parsednxs.AddDatData(RawDat(datfile))
                database.SaveDatFile(parsednxs.dat_data[-1].OutputDatDictForFile())       # save and database the raw
                database.insertData(parsednxs.ReturnSQLDict(datfile))                     # individual dat files
            kind_of_sample = dat_manager.AddParsedNXS(parsednxs)

            if kind_of_sample == 'new buffer':
                log.info('New buffer detected, clearing buffer and sample arrays.')
                buffers = Averaging()
                samples = []
                buffers.AddParsedNXS(parsednxs)

            elif kind_of_sample == 'new sec buffer':
                #start a new zip file and add the raw dats to it
                zippy.Reset()
                zippy.AddParsednxs(parsednxs)
                log.info('New sec buffer detected, clearing buffer and sample arrays.')
                buffers = Averaging()
                samples = []
                buffers.AddParsedNXS(parsednxs)
    
            elif kind_of_sample == 'repeat robot buffer':
                log.info('Repeat robot buffer detected')
                buffers.AddParsedNXS(parsednxs)
                if len(samples) > 0:
                    #average the buffers
                    buffers.RejectOutliers()
                    av_buffer = buffers.Average()
                    database.insertData(av_buffer[1])
                    database.SaveDatFile(av_buffer[0])
                    #average the samples
                    for sample in samples:
                        sample.RejectOutliers()
                        av_sample = sample.Average()
                        database.insertData(av_sample[1])
                        database.SaveDatFile(av_sample[0])
                        #subtract new buffer from sample
                        subtraction = Subtraction()
                        subtraction.AddSample(sample)
                        subtraction.AddBuffer(buffers)
                        sub_sample = subtraction.Subtract()
                        database.insertData(sub_sample[1])
                        database.SaveDatFile(sub_sample[0])
    
            elif kind_of_sample == 'new robot sample':
                log.info('found a robot sample')
                #average the buffers
                buffers.RejectOutliers()
                av_buffer = buffers.Average()
                database.insertData(av_buffer[1])
                database.SaveDatFile(av_buffer[0])
                #average THIS sample
                sample = Averaging()
                sample.AddParsedNXS(parsednxs)
                samples.append(sample)
                sample.RejectOutliers()
                av_sample = sample.Average()
                database.insertData(av_sample[1])
                database.SaveDatFile(av_sample[0])
                #subtract THIS sample
                subtraction = Subtraction()
                subtraction.AddSample(sample)
                subtraction.AddBuffer(buffers)
                sub_sample = subtraction.Subtract()
                database.insertData(sub_sample[1])
                database.SaveDatFile(sub_sample[0])
    
            elif kind_of_sample == 'repeat sec buffer': 
                log.info('Repeat SEC buffer detected, added to buffers in memory')
                zippy.AddParsednxs(parsednxs)
                if buffers.TestSimilar(parsednxs.dat_data[0].ReturnColumn('I')):
                    buffers.AddParsedNXS(parsednxs)
                    buffers.LimitToWindowSize()
    
            elif kind_of_sample == 'new sec sample':
                log.info('found a new SEC sample')
                zippy.Reset()
                zippy.AddParsednxs(parsednxs)
                #average the buffers
                #log.info('dealing with buffers first')
                #buffers.RejectOutliers()
                #av_buffer = buffers.Average()
                #database.insertData(av_buffer[1])
                #database.SaveDatFile(av_buffer[0])
                #average THIS sample
                #log.info('now dealing with sample')
                #sample = Averaging()
                #sample.AddParsedNXS(parsednxs)
                #sample.RejectOutliers()
                #av_sample = sample.Average()
                #database.insertData(av_sample[1])
                #database.SaveDatFile(av_sample[0])
                #subtract EACH INDIVIDUAL dat file in THIS nxs
                #log.info('now subtracting buffer from sample')
                #subtraction = Subtraction()
                #subtraction.AddSample(sample)
                #subtraction.AddBuffer(buffers)
                #sub_sample = subtraction.Subtract()
                #database.insertData(sub_sample[1])
                #database.SaveDatFile(sub_sample[0])
    
            elif kind_of_sample == 'manual collection or scan':
                log.info('manual collection or scan detected, will ignore')
            else:
                log.error('AddParsedNXS returned something unexpected')
    
        
    if len(nxsfiles) == 0:
        log.info('Polled fileserver, no new files')
        time.sleep(10)
    
log.info('Pypline terminated cleanly')
