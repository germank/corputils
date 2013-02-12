#!/usr/bin/env python2.7
import argparse
import fileinput
import re
import os
import sys

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('filenames', default='-', nargs='+')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-t', dest='text_count', 
        help='number of texts to group together', type=int)
    group.add_argument('-s', dest='sentence_count', 
        help='number of sentences to group together (breaks at text markers)',
        type=int)
    parser.add_argument('-o', dest='out_dir',
        help='output_dir', default='.')
    parser.add_argument('-f', dest='out_filename', 
        help='output filename prefix', default='out')

    args = parser.parse_args()

    n = 1 #file index
    t = 0 #texts written
    s = 0 #sentences written

    if not args.text_count and not args.sentence_count:
        args.text_count = 1
    
    if args.text_count:
        #break when either of the limits is attended
        split_here = lambda x_t, x_s: x_t >= args.text_count
    else:
        split_here = lambda x_t, x_s: x_s >= args.sentence_count

    f = open(get_filename(args, n), 'w')
    try:
        written = False
        for line in fileinput.input(args.filenames):
            f.write(line)
            written = True
            if line.startswith('</s>'):
                s += 1
            if line.startswith('</text>'):
                t += 1
                if split_here(t,s):
                    f.close()
                    n += 1
                    sys.stderr.write('.')
                    sys.stderr.flush()
                    if n%20 == 0:
                        sys.stderr.write('[{0}]\n'.format(n))
                        sys.stderr.flush()
                    f = open(get_filename(args, n), 'w')
                    written = False
                    t = 0
                    s = 0 
    finally:
        last_filename = f.name
        f.close()
        if not written:
            os.unlink(last_filename)
def get_filename(args, n):
    return os.path.join(args.out_dir, '{0}.{1}'.format(args.out_filename, n))

if __name__ == '__main__':
    main()
