#!/usr/bin/env python
import logging
LOGGING_FORMAT="%(asctime)s\t%(levelname)s: %(message)s"
LOGGING_DATE_FORMAT="%d-%m-%Y %H:%M:%S"
logging.basicConfig(format=LOGGING_FORMAT, datefmt=LOGGING_DATE_FORMAT, 
                    level=logging.DEBUG)
logger = logging.getLogger()

import argparse
import fileinput
import os
import sys
try:
    import sqlite3
except ImportError:
    logger.warn("Cannot use sqlite to store counts: sqlite3 not available")
import time
#import portalocker
from warnings import filterwarnings
try:
    import MySQLdb
    filterwarnings('ignore', category = MySQLdb.Warning)
except ImportError:
    logger.warn("Cannot use MySql to store counts: MySQLdb not available")
from itertools import repeat, islice
from threading import Thread, RLock
import operator

#logger = logging.getLogger("coocurrence_count")
#logger.setLevel(logging.DEBUG)

logging.basicConfig(level=logging.DEBUG)
MANY = float("inf")
MYSQL_HOST='localhost'
MYSQL_USER='root'
MYSQL_PASS='root'
MYSQL_PORT=3306
BATCH_SIZE = 100
#FIXME: put in unicode o
def main():
    parser = argparse.ArgumentParser(description=
        """Takes a file containing "word_i marker word_j" tuples
        and builds a coocurrence count -core space- for each marker.
        It will also build a sparse matrix for detected
        compositions -peripheral space- 
        (e.g. word_i1<-->word_i2 marker word_j""")
    
    parser.add_argument('input', help="coocurrence tuples", default="-",
        nargs='*')
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('-o', '--output_dir', 
        help="directory where a coocurrence count file will be created "
        "for each pattern", required=True)
    parser.add_argument('-x', '--compose-op', help='string using to identify'
    ' a peripheral space token', default='<-->')
    parser.add_argument('-c', '--cols', help='filter context words')
    parser.add_argument('-r', '--rows', help='filter pivots')
    parser.add_argument('-m', '--many', help='number of records needed to '
                        'start dumping', type=int, default=MANY)
    parser.add_argument('-b','--batch-size', help='size of batchs inserted '
                        'into the DB', type=int, default=BATCH_SIZE)
    parser.add_argument('-e', '--db-engine', help="Destination format", 
                        choices=['mysql', 'sqlite', 'text'], default='text')
    parser.add_argument('--asynchronic', dest='synchronic', 
                        help='continue counting while saving',
                        action='store_false', default=True)
    parser.add_argument('-u', '--mysql_user', help='MYSQL username', default=MYSQL_USER)
    parser.add_argument('-p', '--mysql_passwd', help='MYSQL password', default=MYSQL_PASS)
    parser.add_argument('-H', '--mysql_hostname', help='MYSQL hostname', default=MYSQL_HOST)
    parser.add_argument('-P', '--mysql_port', help='MySQL port', default=MYSQL_PORT, 
                        type=int)
    #TODO: add option to customize dense or sparse

    args = parser.parse_args()
    if args.verbose == 0:
        logger.setLevel(logging.ERROR)
    if args.verbose == 1:
        logger.setLevel(logging.INFO)
    if args.verbose == 2:
        logger.setLevel(logging.DEBUG)
    
    logger.info("Started at {0}".format(str(time.strftime("%d-%m-%Y %H:%M:%S"))))
    #make sure outdir exists
    try:
        os.makedirs(args.output_dir)
    except OSError:
        pass

    if args.cols:
        with open(args.cols) as f_cols:
            cols = [col.rstrip('\n') for col in f_cols]
        col2id = dict((col,i) for i,col in enumerate(cols))
    else:
        cols = None
        col2id = None
    if args.rows:
        with open(args.rows) as f_rows:
            rows = [row.rstrip('\n') for row in f_rows]
        row2id = dict((row,i) for i,row in enumerate(rows))
    else:
        rows = None
        row2id = None


    if args.db_engine == 'mysql':
        per_output_db = args.output_dir +  '_peripheral'
        core_output_db = args.output_dir + '_core'
        per_dest = MySQLDestination(args.hostname, args.port, args.user, args.passwd, 
                          per_output_db, ['cc'], args.batch_size)
        core_dest = MySQLDestination(args.hostname, args.port, args.user, args.passwd, 
                          core_output_db, ['cc'], args.batch_size)
    elif args.db_engine == 'sqlite':
        per_output_db = os.path.join(args.output_dir, 'peripheral.db')
        core_output_db = os.path.join(args.output_dir, 'core.db')
        per_dest = SqliteDestination(per_output_db, args.batch_size)
        core_dest = SqliteDestination(core_output_db, args.batch_size)
    elif args.db_engine == 'text':
        per_output_db = os.path.join(args.output_dir, 'peripheral')
        core_output_db = os.path.join(args.output_dir, 'core')
        per_dest = TextDestination(per_output_db)
        core_dest = TextDestination(core_output_db)
        
    with core_dest, per_dest:
        core = SparseCounter(core_dest, args.many, args.synchronic)
        per = SparseCounter(per_dest, args.many, args.synchronic)

        with Timer() as t_counting:
            try: 
                i=0
                for l in fileinput.input(args.input, 
                                         openhook=fileinput.hook_encoded("utf-8")):
                    i+=1
                    if i%100000 == 0:
                        sys.stdout.write('.')
                    if i%10000000 == 0:
                        sys.stdout.write('\n')
                    sys.stdout.flush()
                    [w1,w2] = l.rstrip('\n').split('\t')
                    if args.compose_op in w1:
                        tg = w1.split(args.compose_op)[1]
                        if (not row2id or tg in row2id) and (not col2id or w2 in col2id):
                            per.count(w1,'c', w2)
                    else:
                        if (not row2id or w1 in row2id) and (not col2id or w2 in col2id):
                            core.count(w1,'c', w2)
            except ValueError:
                logger.error("Error reading line: {0}".format(l))
    
        logger.info("Counting Finished (t={0:.2f})".format(t_counting.interval))
        #wait for any pending saves
        core.join()
        per.join()
        #save residuals
        while len(core)>0:
            core.save()
        while len(per)>0:
            per.save()
    logger.info("Finished at {0}".format(str(time.strftime("%d-%m-%Y %H:%M:%S"))))
        
        
class SparseCounter():
    def __init__(self, output_destination, many, synchronic):
        self.coocurrences = {}
        self.coocurrences_lock = RLock()
        self.saving_thread = None
        self.saving_thread_lock = RLock()
        self.output_destination = output_destination
        self.many = many
        self.synchronic = synchronic
        self.i = 0
    
    def count(self, w1, marker, w2):
        with self.coocurrences_lock:
            if marker not in self.coocurrences:
                self.coocurrences[marker] = {}
            marker_coocurrences = self.coocurrences[marker]
            if (w1,w2) not in marker_coocurrences:
                marker_coocurrences[(w1,w2)] = 0
            marker_coocurrences[(w1,w2)] += 1
            if self.i % 100 == 0:
                if self.synchronic:
                    self.check_dump_sync()
                else:
                    self.check_dump()
                self.i = 0
    
    def __len__(self):
        return sum([len(mc) for mc in self.coocurrences.itervalues()])
    
    def check_dump(self):
        '''Checks whether counts in memory are already too many and a 
        dump is needed. Returns immediately and schedules a thread'''
        with self.saving_thread_lock:
            if len(self) >= self.many\
            and not self.saving_thread:
                logger.info('asking for DB dump')
                self.saving_thread = Thread(target=self.run_dump)
                self.saving_thread.start()
    
    def check_dump_sync(self):
        '''Checks whether counts in memory are already too many and a 
        dump is needed. Waits until finished'''
        if len(self) >= self.many:
            self.save()
                
    def run_dump(self):
        '''main for the saving thread'''
        self.save()
        with self.saving_thread_lock:
            self.saving_thread=None
    
    def join(self):
        '''IMPORTANT: should be called before exiting to ensure that
        there is no pending write'''
        thread_alive = None
        with self.saving_thread_lock:
            if self.saving_thread:
                thread_alive = self.saving_thread
        if thread_alive:
            logger.info('waiting for unfinished saves to end...\t')
            thread_alive.join()
            logger.info('saving thread joined')
    
    def save(self):
        '''Dumps the results to the DB.'''
        N = len(self)
        logger.info("Saving {0} records to {1}" \
                     .format(N, self.output_destination))
        with Timer() as t_save:
            self.output_destination.save(self)
        logger.info("Finished saving {0} records to {1} in {2:.2f} seconds at " 
                     "{3:.2f} records/second ".format(N,
                                                      self.output_destination,
                                                      t_save.interval, 
                                                      N/t_save.interval))
        
class MySQLDestination():
    def __init__(self, host, port, user, passwd, output_db, tables, batch_size ):
        self.output_db = output_db
        self.tables = tables
        self.host = host
        self.user = user
        self.passwd = passwd
        self.port = port
        self.batch_size = batch_size
        
    def __enter__(self):
        self.conn = MySQLdb.connect(host=self.host, user=self.user, 
                                    passwd=self.passwd, port=self.port)
        cur = self.conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS `{0}` DEFAULT CHARACTER SET utf8 ;".format(self.output_db))
        cur.execute("USE {0}".format(self.output_db))
        cur.execute("SET autocommit = 0;")
        for table in self.tables:
            cur.execute(
                    """CREATE  TABLE IF NOT EXISTS `{0}` (
                      `pivot` VARCHAR(150) NOT NULL ,
                      `context` VARCHAR(150) NOT NULL ,
                      `occurrences` INT NULL ,
                      PRIMARY KEY (`pivot`, `context`) ) 
                      ENGINE = InnoDB;""".format(table))
        return self

    def __exit__(self, *args):
        self.conn.close()
        
    def __str__(self):
        return self.output_db
    
    def save(self, counter):
        #keeps a copy and frees the counter
        coocurrences_copy = {}
        with counter.coocurrences_lock:
            for marker in counter.coocurrences.keys():
                coocurrences_copy[marker] = counter.coocurrences[marker]
                del counter.coocurrences[marker]

        cur = self.conn.cursor()
        #repeats in case of deadlock
        while coocurrences_copy:
            for marker in coocurrences_copy.keys():
                marker_coocurrences = coocurrences_copy[marker]             
                marker_table = '{0}'.format(marker)
                query = "insert into {0} values( %s, %s ,%s) " \
                    "on duplicate key update "\
                    "`occurrences` = `occurrences` + VALUES(`occurrences`);" \
                    .format(marker_table)
                
                #sort to prevent deadlocks (still, they happen)
                insert_values = ((w1,w2,c) for (w1,w2),c in \
                                sorted(marker_coocurrences.iteritems(),
                                       key=operator.itemgetter(0)))
                for insert_values_chunk in \
                    split_every(self.batch_size, insert_values.items()):
                    saved = False
                    while not saved:
                        try:
                            self.conn.begin()
                            cur.executemany(query, insert_values_chunk)
                            self.conn.commit()
                            saved = True
                        except MySQLdb.OperationalError, ex:
                            #1213:deadlock detected
                            #1205: lock timeout
                            if ex.args[0] == 1213:
                                #shit happens, we'll try again in the future
                                logger.warning("DEADLOCK detected, retrying")
                            elif  ex.args[0] == 1205:
                                #shit happens, we'll try again in the future
                                logger.warning("TIMEOUT detected, retrying")
                            else:
                                raise
                    del coocurrences_copy[marker]
        cur.close()

class KyotoDestination():
    def __init__(self, output):
        self.output = output
        
    def __enter__(self):
        #ensures file exists
        try:
            os.makedirs(os.path.dirname(self.output))
        except OSError:
            #ok
            pass

        return self

    def __exit__(self, *args):
        pass
    
    def __str__(self):
        return self.output_folder
    
    def save(self, counter):
        #keeps a copy and frees the counter
        coocurrences_copy = {}
        with counter.coocurrences_lock:
            for marker in counter.coocurrences.keys():
                coocurrences_copy[marker] = counter.coocurrences[marker]
                del counter.coocurrences[marker]

        for marker in coocurrences_copy.keys():
            marker_coocurrences = coocurrences_copy[marker]             
            marker_file = os.path.join(self.output_folder, marker)
            
            insert_values = ((w1,w2,c) for (w1,w2),c in \
                            sorted(marker_coocurrences.iteritems(),
                                   key=operator.itemgetter(0)))
            #with portalocker.Lock(marker_file, truncate=None) as out:
            with open(marker_file, 'a') as out:
                for values in insert_values:
                    out.write('{0}\t{1}\t{2}\n'.format(*values))
            del coocurrences_copy[marker]

class TextDestination():
    def __init__(self, output_folder):
        self.output_folder  = output_folder
        
    def __enter__(self):
        #ensures file exists
        try:
            os.makedirs(self.output_folder)
        except OSError:
            #ok
            pass
        return self

    def __exit__(self, *args):
        pass
    
    def __str__(self):
        return self.output_folder
    
    def save(self, counter):
        #keeps a copy and frees the counter
        coocurrences_copy = {}
        with counter.coocurrences_lock:
            for marker in counter.coocurrences.keys():
                coocurrences_copy[marker] = counter.coocurrences[marker]
                del counter.coocurrences[marker]

        for marker in coocurrences_copy.keys():
            marker_coocurrences = coocurrences_copy[marker]             
            marker_file = os.path.join(self.output_folder, marker)
            
            insert_values = ((w1,w2,c) for (w1,w2),c in \
                            sorted(marker_coocurrences.iteritems(),
                                   key=operator.itemgetter(0)))
            #with portalocker.Lock(marker_file, truncate=None) as out:
            with open(marker_file, 'a') as out:
                for values in insert_values:
                    out.write('{0}\t{1}\t{2}\n'.format(*values))
            del coocurrences_copy[marker]

        

class SqliteDestination():
    def __init__(self, output_db, batch_size):
        self.output_db = output_db
        self.batch_size = batch_size
    
    def __str__(self):
        return self.output_db
    
#    def load(self, counter, markers) #FIXME: remove the need for specifying the markers
#        timeout = 60*60*2 #infinitm
#        con = sqlite3.connect(self.output_db,timeout,isolation_level="EXCLUSIVE")
#        cur = con.cursor()
#        #Create tables for each marker before falling into lock
#        #It could become a bug if new markers arise after the tables where
#        #created, but since marker are quite stable it's very unlikely.
#        #The reason for doing this is that the CREATE TABLE frees the lock
#        #and lets other process to take the DB while we where dumping
#        #Any of these firsts queries could lock the DB, but we are not
#        #guaranteed to keep it until we execute the BEGIN EXCLUSIVE
#        cur.execute("PRAGMA synchronous=OFF")
#        cur.execute("PRAGMA count_changes=OFF")
#        cur.execute("PRAGMA journal_mode=OFF")
#        cur.execute("PRAGMA temp_store=MEMORY")
#        con.execute('BEGIN EXCLUSIVE TRANSACTION')
#        logger.debug('DB lock acquired (time to lock={0:.2f} s.)'.format(time.time()-lock_time))
#        #database locked, let's lock the sparse_coocurrences
#        with counter.coocurrences_lock:
#            logger.info('Loading records')
#            for marker in markers:
#                counter.coocurrences[marker] = {}
#                marker_coocurrences = counter.coocurrences[marker]
#                marker_table = '{0}'.format(marker)
#                start_op = time.time()
#                #collect database values
#                select_query = \
#                    "SELECT * FROM {0}".format(marker_table)
#                cur.execute(select_query, params)
#                while 1:
#                    saved = cur.fetchone()
#                    if saved:
#                        try:                            
#                            marker_coocurrences[(saved[0],saved[1])] = \
#                                int(saved[2])
#                        except KeyError:
#                            logger.error("{0} obtained while executing {1}"\
#                                         .format(str(saved)), select_query)
#                    else:
#                        break
#                
#                #for(w1,w2),c in marker_coocurrences.iteritems():
#                #    cur.execute("SELECT * FROM {0} WHERE pivot = ? AND "
#                #                "context =?".format(marker),(w1,w2))
#                #    saved = cur.fetchone()
#                #    if saved:
#                #        marker_coocurrences[(w1,w2)] += int(saved[2])
#                    
#                insert_values = []    
#                for(w1,w2),c in marker_coocurrences.iteritems():
#                    insert_values.append((w1,w2,c))#"coalesce(select occurrences FROM {0} WHERE pivot = '{1}' and context='{2}',0) + {3}".format(marker,w1.replace("'","''"),w2.replace("'","''"),c)))
#                end_op = time.time()
#                logger.debug('Retrieved values for marker {0}. Time consumed={1:.2f}s. Rec/s={2:.2f}'\
#                        .format(marker, end_op-start_op, len(marker_coocurrences)/(end_op-start_op)))
#                start_op = time.time()
#                query = "INSERT OR REPLACE INTO {0} VALUES( ?, ? ,?)".format(marker)
#                try:
#                    cur.executemany(query, insert_values)
#                except sqlite3.OperationalError:
#                    logger.error("Query Failed: {0)".format(query))
#                    raise
#                end_op = time.time()
#                logger.debug('Saved values for marker {0}. Time consumed={1:.2f}s. Rec/s={2:.2f}'\
#                        .format(marker, end_op-start_op, len(marker_coocurrences)/(end_op-start_op)))
#                #clear from memory
#                del counter.coocurrences[marker]
#        con.commit()
#        con.close()
    def save(self, counter):
        timeout = 60*60*2 #infinite
        con = sqlite3.connect(self.output_db,timeout,isolation_level="EXCLUSIVE")
        con.text_factory = str #FIXME: move to unicode
        lock_time = time.time()
        cur = con.cursor()
        #Create tables for each marker before falling into lock
        #It could become a bug if new markers arise after the tables where
        #created, but since marker are quite stable it's very unlikely.
        #The reason for doing this is that the CREATE TABLE frees the lock
        #and lets other process to take the DB while we where dumping
        #Any of these firsts queries could lock the DB, but we are not
        #guaranteed to keep it until we execute the BEGIN EXCLUSIVE
        with counter.coocurrences_lock:
            for marker in counter.coocurrences.keys():
                marker_table = '{0}'.format(marker)
                cur.execute("CREATE TABLE IF NOT EXISTS {0}(pivot text, "
                            "context text, occurrences int, PRIMARY "
                            "KEY(pivot,context))".format(marker_table))
        cur.execute("PRAGMA synchronous=OFF")
        cur.execute("PRAGMA count_changes=OFF")
        cur.execute("PRAGMA journal_mode=OFF")
        cur.execute("PRAGMA temp_store=MEMORY")
        con.execute('BEGIN EXCLUSIVE TRANSACTION')
        logger.debug('DB lock acquired (time to lock={0:.2f} s.)'.format(time.time()-lock_time))
        #database locked, let's lock the sparse_coocurrences
        with counter.coocurrences_lock:
            N_rec = len(counter)
            logger.info('Start dumping {0} records)'.format(N_rec))
            for marker in counter.coocurrences.keys():
                marker_coocurrences = counter.coocurrences[marker]
                marker_table = '{0}'.format(marker)
                start_op = time.time()
                #collect database values
                for marker_cooocurrences_chunk in \
                split_every(self.batch_size, marker_coocurrences.items()):
                    params = []
                    for (w1,w2),c in marker_cooocurrences_chunk:
                        params.append(w1)
                        params.append(w2)
                    select_query = \
                        "SELECT * FROM {0} WHERE {1}".format(marker_table,
                        " OR ".join(repeat("(pivot = ? AND context = ?)", 
                                           len(params)/2)))
                    cur.execute(select_query, params)
                    while 1:
                        saved = cur.fetchone()
                        if saved:
                            try:                            
                                marker_coocurrences[(saved[0],saved[1])] += \
                                    int(saved[2])
                            except KeyError:
                                logger.error("{0} obtained while executing {1}"\
                                             .format(str(saved)), select_query)
                        else:
                            break
                    
                #for(w1,w2),c in marker_coocurrences.iteritems():
                #    cur.execute("SELECT * FROM {0} WHERE pivot = ? AND "
                #                "context =?".format(marker),(w1,w2))
                #    saved = cur.fetchone()
                #    if saved:
                #        marker_coocurrences[(w1,w2)] += int(saved[2])
                    
                insert_values = []    
                for(w1,w2),c in marker_coocurrences.iteritems():
                    insert_values.append((w1,w2,c))#"coalesce(select occurrences FROM {0} WHERE pivot = '{1}' and context='{2}',0) + {3}".format(marker,w1.replace("'","''"),w2.replace("'","''"),c)))
                end_op = time.time()
                logger.debug('Retrieved values for marker {0}. Time consumed={1:.2f}s. Rec/s={2:.2f}'\
                        .format(marker, end_op-start_op, len(marker_coocurrences)/(end_op-start_op)))
                start_op = time.time()
                query = "INSERT OR REPLACE INTO {0} VALUES( ?, ? ,?)".format(marker)
                try:
                    cur.executemany(query, insert_values)
                except sqlite3.OperationalError:
                    logger.error("Query Failed: {0)".format(query))
                    raise
                end_op = time.time()
                logger.debug('Saved values for marker {0}. Time consumed={1:.2f}s. Rec/s={2:.2f}'\
                        .format(marker, end_op-start_op, len(marker_coocurrences)/(end_op-start_op)))
                #clear from memory
                del counter.coocurrences[marker]
        con.commit()
        con.close()
    
    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

def split_every(n, iterable):
    i = iter(iterable)
    piece = list(islice(i, n))
    while piece:
        yield piece
        piece = list(islice(i, n))


class Timer:    
    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.interval = self.end - self.start
        if self.interval == 0.0: #no zero interval
            self.interval = 1.0e-10  

if __name__ == '__main__':
    main()
