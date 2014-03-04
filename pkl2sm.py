#!/usr/bin/env python
import argparse
import pickle
import operator

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('pkl_file')
    parser.add_argument('-v', '--by-value', action='store_true')
    parser.add_argument('-r', '--reverse', action='store_true')
    args = parser.parse_args()

    if args.pkl_file.endswith('.pkl'):
        d = pickle.load(file(args.pkl_file))
    elif args.pkl_file.endswith('.txt'):
        d = dict(map(eval, l.strip().split('\t')) for l in file(args.pkl_file))

    if args.by_value:
        it = sorted(d.iteritems(), key=operator.itemgetter(-1), reverse=args.reverse)
    else:
        it = sorted(d.iteritems(), reverse=args.reverse)
    
    for k,v in it:
        if isinstance(k, basestring):
            print "{0}\t{1}".format(k,v)
        else:
            print "\t".join(k+[v])

if __name__ == '__main__':
    main()
