#!/home/b21user/nathanc/Pypline3/bin/python
'''
Created on Mar 14th, 2016

@author: nathan
'''
from logger import myLogger
import os.path
import sqlite3
import sys
import yaml

class Database(object):
    """Initialise, query, store in sqlite database
    
    This class contains functions to initialise a new SQL database
    from the schema.sql file if no database file exists. The class
    will then make a connection to the database and support querys.
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
        #self.logger = myLogger(os.path.basename(__file__))
        self.logger = myLogger('database')

        self.database_name = None
        self.conn = None
        self.cursor = None

        ###define some parameters
        self.type = 'database'

    def setDatabase(self, dbfile = None):
        try:
            if dbfile == self.database_name:
                self.logger.debug('Using same database as last time, no change needed')
            elif os.path.isdir(os.path.split(dbfile)[0]) and os.path.split(dbfile)[1][-2:] == 'db':
                self.database_name = dbfile
                self.conn = sqlite3.connect(self.database_name)
                self.cursor = self.conn.cursor()
            else:
                raise IOError('database file '+str(dbfile)+' is not valid')
        except Exception as ex:
            self.database_name = self.myconfig['setup']['database']
            self.conn = sqlite3.connect(self.database_name)
            self.cursor = self.conn.cursor()
            template = "An exception of type {0} occured. Arguments:{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            self.logger.error(message)
        
    def getTables(self):
        if self.cursor:
            return [header[0] for header in self.cursor.execute("select name from sqlite_master where type = 'table'").fetchall()]
        else:
            self.logger.error('Tried to get tables without first connecting to database')

    def createDatabase(self):
        try:
            schema_file = open(self.pypline_dir+'/schema.sql', 'r')
            schema = schema_file.read()
            self.cursor.executescript(schema)
            self.logger.info('Created the database structure')
        except:
            self.logger.error('Failed to make the database')
            sys.exit('Failed to make the database')

    def getTableColumns(self, table, returnType=False):
            try:
                if table in self.getTables():
                    if returnType:
                        return [x[2] for x in self.cursor.execute('PRAGMA table_info('+str(table)+')').fetchall()]
                    else:
                        return [x[1] for x in self.cursor.execute('PRAGMA table_info('+str(table)+')').fetchall()]
                else:
                    raise NameError('Table '+str(table)+' does not exist in the database')
            except NameError:
                self.logger.error('Table '+str(table)+' does not exist in the database')
            except Exception as ex:
                template = "An exception of type {0} occured. Arguments:{1!r}"
                message = template.format(type(ex).__name__, ex.args)
                self.logger.error(message)
        
    def insertData(self, insert_dict):
        try:
            if not type(insert_dict) == type({}):
                raise TypeError('insertData function requires a dictionary')

            for key in insert_dict:
                if not key in self.getTables():
                    raise LookupError('Table '+str(key)+' does not exist in the database')
                if not type(insert_dict[key]) == type([]):
                    raise TypeError('each table entry in the insertData function dictionary requires content of type array')
                for item in insert_dict[key]:
                    if not type(item) == type(()):
                        raise TypeError('each table entry in the insertData function dictionary requires a list of tuples')
                    if not len(item) == len(self.getTableColumns(key)):
                        raise IndexError('The number of items provided for insertion into the table '+str(key)+' does not match the number of columns in this table')
                target_data_types = self.getTableColumns(key, True)
                for each_tuple in insert_dict[key]:
                    for index in range(0,len(each_tuple)):
                        item = each_tuple[index]
                        data_type = target_data_types[index]
                        if data_type == 'integer':
                            data_type = type(1)
                        elif data_type == 'real':
                            data_type = type(0.1)
                        else:
                            data_type = type('text')
                        if not type(item) == data_type:
                            if data_type == type(1) and type(item) == type(None):
                                pass
                            else:
                                raise TypeError('tried to insert data of type: '+str(type(item)).split("'")[1]+' into column '+str(index)+' of table '+str(key)+' that needs data of type '+str(data_type).split("'")[1])
                self.cursor.executemany('INSERT OR REPLACE INTO '+key+' VALUES ('+(len(insert_dict[key][0])*'?,')[:-1]+')', insert_dict[key])
                self.logger.info('Inserted '+str(key)+' into database')
            self.conn.commit()
            
                               
        except Exception as ex:
            self.conn.rollback()
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            self.logger.error(message)

    def ReturnVisitNxsFiles(self, visit_object):
        try:
            return [x[0] for x in self.cursor.execute('SELECT nxs_file FROM nxs WHERE visit_id=?', (visit_object.ReturnVisitID(), )).fetchall()]
        except:
            return []

    def ReturnVisits(self):
        return_array = []
        try:
            return [x[0] for x in self.cursor.execute('SELECT visit_id FROM visit').fetchall()]
        except:
            self.logger.info('There were no visits in the database')
            return []

    def ReturnNextIndex(self, visit_object):
        index = len([x[0] for x in self.cursor.execute('SELECT rawdat_file FROM raw_dat INNER JOIN nxs ON nxs.nxs_file = raw_dat.nxs_file WHERE visit_id = ?', (visit_object.ReturnVisitID(),)).fetchall()])+1
        return '{index:{fill}{align}{width}}'.format(index=index, fill='0', align='>', width=int(self.myconfig['settings']['number_digits_in_output_index']))

    def ReturnAvDatIndexRange(self, av_dat_object):
        indeces = [ self.cursor.execute('SELECT output_index FROM raw_dat WHERE rawdat_file=?', (raw_dat,)).fetchone()[0] for raw_dat in av_dat_object.ReturnDatFiles() ]
        return indeces[0]+'-'+indeces[-1]
 
    def SaveDatFile(self, dat_dict):
        outstring_array = []
        try:
            if not type(dat_dict) == type({}):
                raise ValueError('SaveDatFile function needs a dat dictionary as input, see raw_dat.py for example')
            if not 'output_name' in dat_dict.keys():
                raise ValueError('SaveDatFile needs a output_name field in the dat_dict specifying where to write the data')
            else:
                if os.path.split(dat_dict['output_name'])[0] == '' or os.path.isdir(os.path.split(dat_dict['output_name'])[0]):
                    pass
                else:
                    raise ValueError('The output file name you supplied to SaveDatFile does not seem to have a valid path')
            if 'headers' in dat_dict.keys():
                dat_dict['headers'].append('{0: <16}{1: <16}{2: <16}'.format('q(1/A)','Column','Error'))
            else:
                dat_dict['headers'] = [ '{0: <16}{1: <16}{2: <16}'.format('q(1/A)','Column','Error') ]
            for header in dat_dict['headers']:
                outstring_array.append(header)
            if not 'Q' in dat_dict.keys() and not 'I' in dat_dict.keys() and not 'E' in dat_dict.keys():
                raise ValueError('SaveDatFile needs a dat_dict with Q, I and E entries')
            for index in range(0, len(dat_dict['Q'])):
                outstring_array.append('{0:<16.10f}{1:<16.10f}{2:<16.10f}'.format(
                                  dat_dict['Q'][index],
                                  dat_dict['I'][index],
                                  dat_dict['E'][index]))
            outfile = open(dat_dict['output_name'], 'w')
            outfile.write('\n'.join(outstring_array))
            outfile.close()
            self.logger.info('Saved file '+dat_dict['output_name'])
        except Exception as ex:
            template = "An exception of type {0} occured. {1!r}"
            message = template.format(type(ex).__name__, ex.args)
            self.logger.error(message)        

if __name__ == '__main__':
    import visit_id
    job = Database()
    print job.ReturnNextIndex(visit_id.VisitID())
    #job.insertData({'nxs': ('text','text','text','text','text',6,'text',8,'text','text','text')})

