#!/usr/bin/env python
import argparse
import fileinput
import portalocker
import os
import sys
import pickle
import sqlite3
import random
import numpy as np
import time
import logging
import operator
from itertools import *
from datetime import datetime
from threading import Lock, Thread, Condition

#logger = logging.getLogger("coocurrence_count")
#logger.setLevel(logging.DEBUG)
logging.basicConfig(level=logging.DEBUG)
MANY = random.randint(10000,50000) #randomize so they dump at different moments

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
    parser.add_argument('-o', '--output_dir', 
        help="directory where a coocurrence count file will be created "
        "for each pattern", required=True)
    parser.add_argument('-x', '--compose-op', default='<-->')
    parser.add_argument('-c', '--cols')
    parser.add_argument('-r', '--rows')
    #TODO: add option to customize dense or sparse

    args = parser.parse_args()
    sys.stderr.write("Started at {0}\n".format(str(datetime.now())))
    per_output_db = os.path.join(args.output_dir, 'peripheral.db')
    core_output_db = os.path.join(args.output_dir, 'core.db')
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

    #coocurrences = {}

    
    core = SparseCounter(core_output_db)
    per = SparseCounter(per_output_db)

    for l in fileinput.input(args.input, openhook=fileinput.hook_encoded("utf-8")):
        [w1,marker,w2] = l.rstrip('\n').split('\t')
        if args.compose_op in w1:
            tg = w1.split(args.compose_op)[1]
            if (not row2id or tg in row2id) and (not col2id or w2 in col2id):
                per.count(w1, marker, w2)
        else:
            if (not row2id or w1 in row2id) and (not col2id or w2 in col2id):
                core.count(w1, marker, w2)

    sys.stderr.write('\n')
    logging.info("finished counting")
    #wait for any pending saves
    core.join()
    per.join()
    #save residuals
    if len(core)>0:
        sys.stderr.write('saving core matrix...\t')
        core.save()
        sys.stderr.write('done\n')
    if len(per)>0:
        sys.stderr.write('saving peripheral matrix...\t')
        per.save()
        sys.stderr.write('done\n')
    sys.stderr.write("Finished at {0}\n".format(str(datetime.now())))
        
        
class SparseCounter():
    def __init__(self, output_db):
        self.coocurrences = {}
        self.coocurrences_lock = Lock()
        self.saving_thread = None
        self.saving_thread_lock = Lock()
        self.output_db = output_db
    
    def count(self, w1, marker, w2):
        with self.coocurrences_lock:
            if marker not in self.coocurrences:
                self.coocurrences[marker] = {}
            marker_coocurrences = self.coocurrences[marker]
            if (w1,w2) not in marker_coocurrences:
                marker_coocurrences[(w1,w2)] = 0
            marker_coocurrences[(w1,w2)] += 1
            self.check_dump()
    
    def __len__(self):
        return sum([len(mc) for mc in self.coocurrences.itervalues()])
    
    def check_dump(self):
        with self.saving_thread_lock:
            if len(self) >= MANY\
            and not self.saving_thread:
                logging.info('asking for DB dump (records={0})'.format(len(self)))
                self.saving_thread = Thread(target=self.run_dump)
                self.saving_thread.start()
                
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
        logging.info('waiting for unfinished saves to end...\t')
        if thread_alive:
            thread_alive.join()
        logging.info('saving thread joined')
    
    def save(self):
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
        with self.coocurrences_lock:
            for marker in self.coocurrences.keys():
                marker_table = '{0}'.format(marker)
                cur.execute("CREATE TABLE IF NOT EXISTS {0}(pivot text, "
                            "context text, occurrences int, PRIMARY "
                            "KEY(pivot,context))".format(marker_table))
        cur.execute("PRAGMA synchronous=OFF")
        cur.execute("PRAGMA count_changes=OFF")
        cur.execute("PRAGMA journal_mode=MEMORY")
        cur.execute("PRAGMA temp_store=MEMORY")
        con.execute('BEGIN EXCLUSIVE TRANSACTION')
        logging.debug('DB lock acquired (time to lock={0:.2f} s.)'.format(time.time()-lock_time))
        #database locked, let's lock the sparse_coocurrences
        with self.coocurrences_lock:
            start=time.time()
            N_rec = len(self)
            logging.info('Start dumping {0} records)'.format(N_rec))
            for marker in self.coocurrences.keys():
                marker_coocurrences = self.coocurrences[marker]
                marker_table = '{0}'.format(marker)
                start_op = time.time()
                #collect database values
                batch_size = 100
                for marker_cooocurrences_chunk in \
                split_every(batch_size, marker_coocurrences.items()):
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
                            marker_coocurrences[(saved[0],saved[1])] += int(saved[2])
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
                logging.debug('Retrieved values for marker {0}. Time consumed={1:.2f}s. Rec/s={2:.2f}'\
                        .format(marker, end_op-start_op, len(marker_coocurrences)/(end_op-start_op)))
                start_op = time.time()
                query = "INSERT OR REPLACE INTO {0} VALUES( ?, ? ,?)".format(marker)
                try:
                    cur.executemany(query, insert_values)
                except sqlite3.OperationalError:
                    logging.error("Query Failed: {0)".format(query))
                    raise
                end_op = time.time()
                logging.debug('Saved values for marker {0}. Time consumed={1:.2f}s. Rec/s={2:.2f}'\
                        .format(marker, end_op-start_op, len(marker_coocurrences)/(end_op-start_op)))
                #clear from memory
                del self.coocurrences[marker]
            
            end=time.time()    
            logging.info('Dumping finished. Time consumed={0:.2f}s. Rec/s={1:.2f}'\
                        .format(end-start, N_rec/(end-start)))
        con.commit()
        con.close()

    
def save_dense_matrix(m, outdir, fname):
    outfile = os.path.join(outdir, fname)
    #make sure fname exists
    with open(outfile, 'a'):
        pass
    #save matrix
    with open(outfile, 'r+') as fout:
        portalocker.lock(fout, portalocker.LOCK_EX)
        not_empty = fout.read(1)
        fout.seek(0)
        if not_empty:
            curr_mat = pickle.load(fout)
            m += curr_mat
            fout.seek(0)
        pickle.dump(m, fout)
        
        #dumb idea
        #for i in xrange(m.shape[0]):
        #    current_pos = fout.tell()
        #    l = fout.readline()
        #    if l:
        #        current_row = np.array(l.split('\t'))
        #        assert len(current_row) == m.shape[1]
        #    else:
        #        current_row = np.zeros(m.shape[1])
        #    current_row += m[i]
        #    l.seek(current_pos)

def split_every(n, iterable):
    i = iter(iterable)
    piece = list(islice(i, n))
    while piece:
        yield piece
        piece = list(islice(i, n))
def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


    

if __name__ == '__main__':
    main()
