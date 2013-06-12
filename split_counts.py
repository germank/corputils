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
    parser = argparse.ArgumentParser(description='Splits a file into multiple'
    ' files. Each line is matched against a expression which determines a '
    ' destination filename')
    parser.add_argument('--hash_pattern', '-p', help='A regular expression that'
    'matches the part of the line that will be used in the destination'
    ' filename. By default it uses everything in the line upto the sign "<"',
    default='.*?(?=<)')
    parser.add_argument('--filenames_extension', '-e', default='.sm')
    parser.add_argument('--output_dir', '-o', default='.')
    parser.add_argument('filenames', nargs='+')

    args = parser.parse_args()

    assert all((fn.endswith(args.filenames_extension) for fn in
    args.filenames)), 'The provided files do not end with extension {0}. Fix '\
    'it or change the extension using the switch'
    ' -e'.format(args.filenames_extension)
    
    
    pattern = re.compile(args.hash_pattern)
    subfilenames_format = os.path.join(args.output_dir, 
        args.filenames[0][:-len(args.filenames_extension)] + '.{0}' +
        args.filenames_extension)

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
    
