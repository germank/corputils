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
from datetime import datetime
from threading import Lock, Thread, Condition

MANY = random.randint(5000,10000) #randomize so they dump at different moments


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
    parser.add_argument('-c', '--cols', required=True)
    parser.add_argument('-r', '--rows', required=True)
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
    else:
        cols = None
    if args.rows:
        with open(args.rows) as f_rows:
            rows = [row.rstrip('\n') for row in f_rows]
    else:
        rows = None

    col2id = dict((col,i) for i,col in enumerate(cols))
    row2id = dict((row,i) for i,row in enumerate(rows))
    #coocurrences = {}

    
    core = SparseCounter(core_output_db)
    per = SparseCounter(per_output_db)

    for l in fileinput.input(args.input):
        [w1,marker,w2] = l.rstrip('\n').split('\t')
        if args.compose_op in w1:
            tg = w1.split(args.compose_op)[1]
            if (not row2id or tg in row2id) and (not col2id or w2 in col2id):
                per.count(w1, marker, w2)
        else:
            if (not row2id or w1 in row2id) and (not col2id or w2 in col2id):
                core.count(w1, marker, w2)

    sys.stderr.write('\n')

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
        sys.stderr.write('waiting to write in sparse matrix...\t')
        if thread_alive:
            thread_alive.join()
        sys.stderr.write('done\n')
    
    def save(self):
        timeout = 60*60*2 #infinite
        con = sqlite3.connect(self.output_db,timeout)
        con.execute('BEGIN EXCLUSIVE TRANSACTION')
        #database locked, let's lock the sparse_coocurrences
        with self.coocurrences_lock:
            for marker in self.coocurrences.keys():
                marker_sparse_coocurrences = self.coocurrences[marker]
                marker_table = '{0}'.format(marker)
                #con.execute("BEGIN EXCLUSIVE TRANSACTION")
                con.execute("CREATE TABLE IF NOT EXISTS {0}(pivot text, "
                            "context text, occurrences int, PRIMARY "
                            "KEY(pivot,context))".format(marker_table))
                for(w1,w2),c in marker_sparse_coocurrences.iteritems():
                    query = ("INSERT OR REPLACE INTO {0} VALUES('{1}','{2}', "
                    "coalesce((select occurrences from {0} WHERE pivot ='{1}' AND "
                    "context='{2}'),0) + {3})").format(
                            marker_table, w1.replace("'", "''"), 
                            w2.replace("'", "''"), c)
                    try:
                        con.execute(query)
                    except sqlite3.OperationalError:
                        print query
                        raise
                #clear from memory
                del self.coocurrences[marker]
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



    

if __name__ == '__main__':
    main()
