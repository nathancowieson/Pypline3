import os.path
import yaml

from visit_id import VisitID
from poll_directory import PollFileSystem
from database import Database
from logger import myLogger
from nxs import ParseNXS
from raw_dat import RawDat
from dat_manager import DatManager
from averaging import Averaging
#START A LOG FILE
log = myLogger(os.path.basename(__file__))

#GET THE CONFIG FILE
pypline_dir = os.path.dirname(os.path.realpath(__file__))
with open(pypline_dir+'/config.yaml', 'r') as ymlfile:
    myconfig = yaml.load(ymlfile)

#SET SOME PARAMETERS
buffers = Averaging()
samples = []

#CONNECT TO THE SQLITE DATABASE
database = Database()

if not os.path.isfile(myconfig['setup']['database']):
    log.info('no database found, starting a new one')
    database.createDatabase()

#DEFINE THE VISIT
visit = VisitID('sw14620-1')
visit.MakeOutputDirs()

#A VARIABLE FOR MAKING DECISIONS ABOUT ROBOT BUFFERS
previous_scan_type = None

#MAKE A VISIT ENTRY IN THE DATABASE IF ITS NOT THERE ALREADY
if not visit.ReturnVisitID() in database.ReturnVisits():
    log.info('Entered visit: '+visit.ReturnVisitID()+' to the database')
    database.insertData(visit.ReturnSQLDict())#database the visit

#CONNECT TO THE FILE SYSTEM
files = PollFileSystem(visit)

#AN OBJECT TO HANDLE AVERAGING AND SUBTRACTING
dat_manager = DatManager()

#PARSE THE NEW NXS FILES
for nxsfile in sorted( list( set(files.ReturnNxsFiles()) - set(database.ReturnVisitNxsFiles(visit)) ) )[17:]:
    log.info('------------NEW NXS------------')
    parsednxs = ParseNXS(database, visit, nxsfile)
    database.insertData(parsednxs.ReturnSQLDict('nxs'))
    if parsednxs.WaitForDatFiles():
        for datfile in parsednxs.ReturnDatFiles():
            parsednxs.AddDatData(RawDat(datfile))
            database.SaveDatFile(parsednxs.dat_data[-1].OutputDatDictForFile())       # save and database the raw
            database.insertData(parsednxs.ReturnSQLDict(datfile))                     # individual dat files
        kind_of_sample = dat_manager.AddParsedNXS(parsednxs)
        if kind_of_sample == 'new buffer':
            buffers = Averaging()
            samples = []
            buffers.AddParsedNXS(parsednxs)

        elif kind_of_sample == 'repeat robot buffer':
            buffers.AddParsedNXS(parsednxs)
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
                #subtract each sample in memory

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

        elif kind_of_sample == 'repeat sec buffer': 
            log.info('Repeat SEC buffer detected, added to buffers in memory')
            buffers.AddParsedNXS(parsednxs)
            buffers.LimitToWindowSize()

        elif kind_of_sample == 'new sec sample':
            log.info('found a new SEC sample')
            #average the buffers
            buffers.RejectOutliers()
            av_buffer = buffers.Average()
            database.insertData(av_buffer[1])
            database.SaveDatFile(av_buffer[0])
            #average THIS sample
            sample = Averaging()
            sample.AddParsedNXS(parsednxs)
            sample.RejectOutliers()
            av_sample = sample.Average()
            database.insertData(av_sample[1])
            database.SaveDatFile(av_sample[0])

            #subtract EACH INDIVIDUAL dat file in THIS nxs
        elif kind_of_sample == 'manual collection or scan':
            log.info('manual collection or scan detected, will ignore')
        else:
            log.error('AddParsedNXS returned something unexpected')

            
    dat_manager
