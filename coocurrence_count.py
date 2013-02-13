#!/usr/bin/env python
import argparse
import fileinput
import portalocker
import os
import pickle
import numpy as np

#MANY_COUNTS_PER_MARKER = 100000

def main():
    parser = argparse.ArgumentParser(description=
        """Takes a file containing "word_i marker word_j" tuples
        and builds a coocurrence count for each marker""")
    
    parser.add_argument('input', help="coocurrence tuples", default="-")
    parser.add_argument('-o', '--output_dir', 
        help="directory where a coocurrence count file will be created "
        "for each pattern", required=True)
    parser.add_argument('-c', '--cols', required=True)
    parser.add_argument('-r', '--rows', required=True)
    #TODO: add option to customize dense or sparse

    args = parser.parse_args()
    count_dense(args)

def count_dense(args):
    with open(args.cols) as f_cols:
        cols = [col.rstrip('\n') for col in f_cols]
    with open(args.rows) as f_rows:
        rows = [row.rstrip('\n') for row in f_rows]

    col2id = dict((col,i) for i,col in enumerate(cols))
    row2id = dict((row,i) for i,row in enumerate(rows))
    coocurrences = {}
    for l in fileinput.input(args.input):
        [w1,marker,w2] = l.rstrip('\n').split('\t')
        marker_coocurrences = coocurrences.setdefault(marker,
            np.zeros((len(rows),len(cols)),dtype=np.int))
        if w1 in row2id and w2 in col2id:
            marker_coocurrences[(row2id[w1],col2id[w2])] += 1

    for marker,marker_coocurrences in coocurrences.iteritems():
        save_dense_matrix(marker_coocurrences,
            args.output_dir, marker + '.pkl')

def save_dense_matrix(m, outdir, fname):
    #make sure outdir exists
    try:
        os.makedirs(outdir)
    except OSError:
        pass
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







 #   coocurrences = {}
 #   for l in fileinput.input(args.input):
 #       [w1,marker,w2] = l.rstrip('\n').split('\t')
 #       marker_coocurrences =coocurrences.setdefault(marker, dict())
 #       if (w1,w2) not in marker_coocurrences:
 #           marker_coocurrences[(w1,w2)] = 0
 #       marker_coocurrences[(w1,w2)] += 1
 #       check_many_tuples(


    

if __name__ == '__main__':
    main()
