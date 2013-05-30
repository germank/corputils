#!/usr/bin/env python
'''
Created on May 30, 2013

@author: german
'''
import argparse
import fileinput
import logging
import re
import os
import sys
from cache import lru_cache

@lru_cache()
def open_file(filename):
    return open(filename, 'w')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--hash_pattern', '-p', default='.*?(?=<)')
    parser.add_argument('--filenames_extension', '-e', default='.sm')
    parser.add_argument('filenames', nargs='+')

    args = parser.parse_args()
    
    pattern = re.compile(args.hash_pattern)
    subfilenames_format = args.filenames[0][:-len(args.filenames_extension)] + '.{0}' + args.filenames_extension

    for line in fileinput.input(args.filenames):
        m = pattern.match(line)
        if m:
            hash_name = m.group(0)
            subfilename = subfilenames_format.format(hash_name)
            f = open_file(subfilename)
            f.write(line)
        else:
            logging.warn('{0} didn\'t match the pattern'.format(line))


if __name__ == '__main__':
    main()
    