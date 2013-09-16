#!/usr/bin/env python
import sys
import fileinput
import argparse

import argparse
import fileinput
import os
import sys

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('filenames', nargs='*')

    args = parser.parse_args()

    progress = 0
    freqs = {}
    for l in fileinput.input(args.filenames):
        if l not in freqs:
            freqs[l] = 0
        freqs[l] += 1
        progress += 1
        if progress % 100000 == 0:
            sys.stderr.write('.')
        if progress % 10000000 == 0:
            sys.stderr.write('\n')
            sys.stderr.flush()

    for w,f in sorted(freqs.iteritems(), key=lambda (k,v): -v):
        print "{0}\t{1}".format(w.rstrip('\n'),f)


                                


if __name__ == '__main__':
    main()
