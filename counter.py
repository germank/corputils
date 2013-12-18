from collections import Counter
from threading import Thread, Lock
import logging
import itertools

class PersistentCounter():
    def __init__(self, output_destination, buf_size, synchronic):
        self.output_destination = output_destination
        self.buf_size = buf_size
        self.synchronic = synchronic
    
    def count(self, target, feature):
        with self.cooccurrences_lock:
            self.cooccurrences[(target, feature)] += 1
            self.i += 1
            if self.i % (self.buf_size / 100 + 1) == 0:
                if self.synchronic:
                    self.check_dump_sync()
                else:
                    self.check_dump()
                self.i = 0
    
    def __len__(self):
        return len(self.cooccurrences)

    def open(self):
        self.cooccurrences = Counter()
        self.i = 0
        self.saving_thread = None
        self.cooccurrences_lock = Lock()
        self.saving_thread_lock = Lock()
        return self.output_destination.open()
    
    def close(self):
        self.join()
        while len(self) >0:
            self.save()
        self.cooccurrences_lock = None
        self.saving_thread_lock = None
        return self.output_destination.close()

    def check_dump(self):
        '''Checks whether counts in memory are already too buf_size and a 
        dump is needed. Returns immediately and schedules a thread'''
        with self.saving_thread_lock:
            if len(self) >= self.buf_size\
            and not self.saving_thread:
                logging.info('asking for DB dump')
                self.saving_thread = Thread(target=self.run_dump)
                self.saving_thread.start()
    
    def check_dump_sync(self):
        '''Checks whether counts in memory are already too buf_size and a 
        dump is needed. Waits until finished'''
        if len(self) >= self.buf_size:
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
            logging.info('waiting for unfinished saves to end...\t')
            thread_alive.join()
            logging.info('saving thread joined')
    
    def save(self):
        '''Dumps the results to the DB.'''
        N = len(self)
        logging.info("Saving {0} records to {1}" \
                     .format(N, self.output_destination))
        with Timer() as t_save:
            self.output_destination.save(self)
        logging.info("Finished saving {0} records to {1} in {2:.2f} seconds at " 
                     "{3:.2f} records/second ".format(N,
                                                      self.output_destination,
                                                      t_save.interval, 
                                                      N/t_save.interval))
try:
    import kyototycoon as kt
    import os
    class KyotoDestination():
        def __init__(self, db_mgr):
            self.host = db_mgr.get_host()
            self.port = db_mgr.get_port()
            
        def open(self):
            self.db = kt.KyotoTycoon(self.host, self.port)
            return self

        def close(self):
            self.db.close()
            self.db = None
        
        def __str__(self):
            return self.host
        
        def save(self, counter):
            #keeps a copy and frees the counter
            with counter.cooccurrences_lock:
                cooccurrences_copy =counter.cooccurrences.copy()
                counter.cooccurrences.clear()

            self.db.play_script('incrementbulk',
                map(lambda (k,v): (repr(k),str(v)), cooccurrences_copy.iteritems()))

        def load(self):
            return map(lambda (k,v): (eval(k), int(v)), 
                self.db.play_script('list',{}).iteritems())
            
except ImportError, e:
    logging.warn('{0}: KyotoCabinet Destination is not'
    ' available'.format(e.message))

#try:
#    import kyotocabinet as kc
#    import os
#    class KyotoDestination():
#        def __init__(self, output):
#            self.output = output
#            
#        def open(self):
#            #ensures file exists
#            try:
#                os.makedirs(os.path.dirname(self.output))
#            except OSError:
#                #ok
#                pass
#            self.db = kc.DB()
#            self.db.open(self.output, kc.DB.OWRITER | kc.DB.OCREATE)
#            return self
#
#        def close(self):
#            self.db.close()
#            self.db = None
#        
#        def __str__(self):
#            return self.output
#        
#        def save(self, counter):
#            #keeps a copy and frees the counter
#            with counter.cooccurrences_lock:
#                cooccurrences_copy =counter.cooccurrences.copy()
#                counter.cooccurrences.clear()
#
#            self.db.begin_transaction()
#            for k,v in cooccurrences_copy.iteritems():
#                self.db.increment(k, v, 0)
#            self.db.end_transaction()
#except ImportError, e:
#    logging.warn('{0}: KyotoCabinet Destination is not'
#    ' available'.format(e.message))
try:
    import sqlite3
    class SqliteDestination():
        def __init__(self, output_db, batch_size):
            self.output_db = output_db
            self.batch_size = batch_size
        
        def __str__(self):
            return self.output_db
        
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
            with counter.cooccurrences_lock:
                for marker in ['cc']:
                    marker_table = '{0}'.format(marker)
                    cur.execute("CREATE TABLE IF NOT EXISTS {0}(pivot text, "
                                "context text, occurrences int, PRIMARY "
                                "KEY(pivot,context))".format(marker_table))
            cur.execute("PRAGMA synchronous=OFF")
            cur.execute("PRAGMA count_changes=OFF")
            cur.execute("PRAGMA journal_mode=OFF")
            cur.execute("PRAGMA temp_store=MEMORY")
            con.execute('BEGIN EXCLUSIVE TRANSACTION')
            logging.debug('DB lock acquired (time to lock={0:.2f} s.)'.format(time.time()-lock_time))
            #database locked, let's lock the sparse_cooccurrences
            with counter.cooccurrences_lock:
                N_rec = len(counter)
                logging.info('Start dumping {0} records)'.format(N_rec))
                for marker in ['cc']:
                    marker_cooccurrences = counter.cooccurrences
                    marker_table = '{0}'.format(marker)
                    start_op = time.time()
                    #collect database values
                    for marker_cooocurrences_chunk in \
                    split_every(self.batch_size, marker_cooccurrences.items()):
                        params = []
                        for (w1,w2),c in marker_cooocurrences_chunk:
                            params.append(w1)
                            params.append(w2)
                        select_query = \
                            "SELECT * FROM {0} WHERE {1}".format(marker_table,
                            " OR ".join(itertools.repeat("(pivot = ? AND context = ?)", 
                                               len(params)/2)))
                        cur.execute(select_query, params)
                        while 1:
                            saved = cur.fetchone()
                            if saved:
                                try:                            
                                    marker_cooccurrences[(saved[0],saved[1])] += \
                                        int(saved[2])
                                except KeyError:
                                    logging.error("{0} obtained while executing {1}"\
                                                 .format(str(saved)), select_query)
                            else:
                                break
                        
                    insert_values = []    
                    for(w1,w2),c in marker_cooccurrences.iteritems():
                        insert_values.append((w1,w2,c))
                    end_op = time.time()
                    logging.debug('Retrieved values for marker {0}. Time consumed={1:.2f}s. Rec/s={2:.2f}'\
                            .format(marker, end_op-start_op, len(marker_cooccurrences)/(end_op-start_op)))
                    start_op = time.time()
                    query = "INSERT OR REPLACE INTO {0} VALUES( ?, ? ,?)".format(marker)
                    try:
                        cur.executemany(query, insert_values)
                    except sqlite3.OperationalError:
                        logging.error("Query Failed: {0)".format(query))
                        raise
                    end_op = time.time()
                    logging.debug('Saved values for marker {0}. Time consumed={1:.2f}s. Rec/s={2:.2f}'\
                            .format(marker, end_op-start_op, len(marker_cooccurrences)/(end_op-start_op)))
                    #clear from memory
                    counter.cooccurrences.clear()
            con.commit()
            con.close()
        
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    def split_every(n, iterable):
        i = iter(iterable)
        piece = list(itertools.islice(i, n))
        while piece:
            yield piece
            piece = list(itertools.islice(i, n))

except ImportError, e:
    logging.warn('{0}: Sqlite Destination is not'
    ' available'.format(e.message))
    
import time
class Timer:    
    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.interval = self.end - self.start
        if self.interval == 0.0: #no zero interval
            self.interval = 1.0e-10  
