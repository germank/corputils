#!/usr/bin/env python2.7
import sys
import os
import argparse
import random
import string
import heapq

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('filenames', nargs='+')
    args = parser.parse_args()
    files = map(open, args.filenames)
    for l in heapq.merge(*files):
        print l,

if __name__ == '__main__':
    main()

