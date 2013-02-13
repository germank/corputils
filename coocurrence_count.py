#!/usr/bin/env python
import argparse
import fileinput
import portalocker
import os
import sys
import pickle
import sqlite3
import numpy as np
from threading import Lock, Thread, Condition

MANY = 100000
saving_thread = None

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
    per_output_db = os.path.join(args.output_dir, 'peripheral.db')
    core_output_db = os.path.join(args.output_dir, 'core.db')
    #make sure outdir exists
    try:
        os.makedirs(args.output_dir)
    except OSError:
        pass

    with open(args.cols) as f_cols:
        cols = [col.rstrip('\n') for col in f_cols]
    with open(args.rows) as f_rows:
        rows = [row.rstrip('\n') for row in f_rows]

    col2id = dict((col,i) for i,col in enumerate(cols))
    row2id = dict((row,i) for i,row in enumerate(rows))
    coocurrences = {}
    peripheral_coocurrences = {}
    peripheral_coocurrences_lock = Lock()
    core_coocurrences = {}
    core_coocurrences_lock = Lock()
    
    i=0
    for l in fileinput.input(args.input):
        i+=1
        if i % 1000 == 0:
            sys.stderr.write('.')
            if i % 80000 == 0:
                sys.stderr.write('\n')
        [w1,marker,w2] = l.rstrip('\n').split('\t')
        #counts core space occurrences
        #if marker not in coocurrences:
        #    coocurrences[marker] = np.zeros((len(rows),len(cols)),dtype=np.int)
        #marker_coocurrences = coocurrences[marker]
        #if w1 in row2id and w2 in col2id:
        #    marker_coocurrences[(row2id[w1],col2id[w2])] += 1
        #counts peripheral space occurrences
        if w1 in row2id and w2 in col2id:
            save_sparse(core_coocurrences_lock, 
                        core_coocurrences, w1, marker, w2)
            ask_writing_if_many(core_coocurrences_lock,
                                core_coocurrences, 
                                core_output_db)
        if args.compose_op in w1 and w1.split(args.compose_op)[1] in row2id:
            save_sparse(peripheral_coocurrences_lock, 
                        peripheral_coocurrences, w1, marker, w2)
            ask_writing_if_many(peripheral_coocurrences_lock,
                                peripheral_coocurrences, 
                                per_output_db)
    sys.stderr.write('\n')
    for marker,marker_coocurrences in coocurrences.iteritems():
        sys.stderr.write('writing {0}.pkl...\t'.format(marker))
        save_dense_matrix(marker_coocurrences,
            args.output_dir, marker + '.pkl')
        sys.stderr.write('done\n')
    #wait for any pending saves
    with peripheral_coocurrences_lock:
        if saving_thread:
            sys.stderr.write('wating for write in sparse matrix...\t')
            saving_thread.join()
            sys.stderr.write('done\n')
    #no more threads now, but there can be some residual
    if len(peripheral_coocurrences) > 0:
        sys.stderr.write('saving peripheral matrix...\t')
        save_sparse_matrix(peripheral_coocurrences_lock,
                           peripheral_coocurrences, per_output_db)
        sys.stderr.write('done\n')
    if len(peripheral_coocurrences) > 0:
        sys.stderr.write('saving core matrix...\t')
        save_sparse_matrix(core_coocurrences_lock,
                           core_coocurrences, core_output_db)
        sys.stderr.write('done\n')
        
def save_sparse(peripheral_coocurrences_lock, 
                peripheral_coocurrences, w1, marker, w2):
    with peripheral_coocurrences_lock:
        marker_peripheral_coocurrences = \
                peripheral_coocurrences.setdefault(marker, dict())
        if (w1,w2) not in marker_peripheral_coocurrences:
            marker_peripheral_coocurrences[(w1,w2)] = 0
        marker_peripheral_coocurrences[(w1,w2)] += 1


def ask_writing_if_many(sparse_coocurrences_lock,
                        sparse_coocurrences, output_db):
    with sparse_coocurrences_lock:
        if len(sparse_coocurrences) < MANY:
            return
        if not saving_thread:
            saving_thread = Thread(target=save_sparse_matrix, 
                                   args=[sparse_coocurrences_lock,
                                     sparse_coocurrences, output_db])
            saving_thread.start()
    
def save_sparse_matrix(sparse_coocurrences_lock, 
                             sparse_coocurrences, output_db):
    con = sqlite3.connect(output_db, isolation_level='EXCLUSIVE')
    con.execute('BEGIN EXCLUSIVE')
    #database locked, let's lock the sparse_coocurrences
    with sparse_coocurrences_lock:
        for marker in sparse_coocurrences.keys():
            marker_sparse_coocurrences = sparse_coocurrences[marker]
            marker_table = '{0}'.format(marker)
            con.execute("CREATE TABLE IF NOT EXISTS {0}(pivot text, "
                        "context text, occurrences int, PRIMARY "
                        "KEY(pivot,context))".format(marker_table))
            for(w1,w2),c in marker_sparse_coocurrences.iteritems():
                query = "INSERT OR REPLACE INTO {0} VALUES('{1}','{2}', "
                "coalesce((select occurrences from {0} WHERE pivot ='{1}' AND "
                "context='{2}'),0) + {3})".format(
                        marker_table, w1.replace("'", "''"), 
                        w2.replace("'", "''"), c)
                try:
                    con.execute(query)
                except sqlite3.OperationalError:
                    print query
                    raise
            #clear from memory
            del sparse_coocurrences[marker]
        con.commit()
        con.close()
        #going away
        #FIXME: shouldn't be the task of this function
        saving_thread = None
    
    
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
