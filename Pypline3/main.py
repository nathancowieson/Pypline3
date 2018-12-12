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
from nexusformat import nexus as nx
import json

#A FUNCTION TO TELL IF SUBDIRS ARE TURNED ON IN THE PROCESSING PIPELINE
#THIS HAS AN EFFECT ON WHERE THE ParseNXS CLASS LOOKS FOR DAT FILES
def returnAreSubdirs(visit_obj=None, default=True):
    returnValue = default
    try:
        pipeline_file = json.loads(open(visit.ReturnVisitDirectory()+'/xml/templates/template.json', 'r').read())['processingPath']
        mynxs = nx.tree.NXFile(pipeline_file, 'r')
        tree = mynxs.readfile()
        for item in tree.entry.process._entries.iteritems():
            try:
                if item[1].name.nxdata == u'Export to Text File':
                    mydata = json.loads(item[1].data.nxdata)
            except:
                pass
        returnValue = mydata['makeFolder']
        if returnValue:
            log.info('Subdirs are switched on for file output in the processing pipeline')
        else:
            log.info('Subdirs are switched off for file output in the processing pipeline')
    except:
        log.error('Could not get from pipeline file if there are subdirectories, using default: '+str(default))
    return returnValue



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
previous_visit = None
subdirs = True

#CONNECT TO THE SQLITE DATABASE
database = Database()

#AN OBJECT TO HANDLE AVERAGING AND SUBTRACTING
dat_manager = DatManager()

#AN OBJECT TO HANDLE THE VISIT
if options.visit_id == 'None':
    visit = VisitID()
    log.info('Visit automatically set to '+str(visit.ReturnVisitID()))
else:
    visit = VisitID(options.visit_id)
    log.info('Visit ID was set manually to '+str(visit.ReturnVisitID()))


polling = 1
while polling:
    #IF POLLING IS NOT SPECIFIED THEN RUN THROUGH ONCE AND STOP
    if not options.poll:
        log.info('Not polling, will run through once and stop')
        polling = 0

    #CHECK CHANGES IN VISIT
    if not previous_visit == visit.ReturnVisitID():
        log.info('Visit changed from: '+str(previous_visit)+' to: '+visit.ReturnVisitID())
        visit.MakeOutputDirs()
        previous_visit = visit.ReturnVisitID()
        subdirs = returnAreSubdirs(visit)

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
        tries = 0
        while tries < 4:
            if parsednxs.success:
                break
            else:
                tries += 1
                time.sleep(5)
                log.error('NXS file failed to parse, waiting 5 seconds')
        if parsednxs.success:
            log.info('Nxs parse successful, waiting on dat files')
            if parsednxs.WaitForDatFiles(subdirs):
                log.info('Found the dat files')
                for datfile in parsednxs.ReturnDatFiles():
                    parsednxs.AddDatData(RawDat(datfile))
                    database.SaveDatFile(parsednxs.dat_data[-1].OutputDatDictForFile())           # save and database the raw
                    database.insertData(parsednxs.ReturnSQLDict(datfile))                         # individual dat files
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
            else:
                log.info(', '.join(parsednxs.ReturnDatFiles()))
                log.error('Dat files were not in found within wait time')
        
    if len(nxsfiles) == 0:
        log.debug('Polled fileserver, no new files')
        time.sleep(10)
    
log.info('Pypline terminated cleanly')

        
