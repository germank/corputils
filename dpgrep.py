#!/usr/bin/env python
'''
Created on Jun 12, 2013

@author: german
'''
import argparse
import fileinput
import os
import re
import sys

from sentence_matchers import *

def main():
    parser = argparse.ArgumentParser(description=
    '''Prints sentences that match the given cirteria''')
    parser.add_argument('corpora', help='files with the parsed corpora',
        default="-", nargs='*')
    #parser.add_argument('-P', '--no-pos', help='don\'t add a POS indicator to '
    #                    'the lemmas', dest='pos', default=True, action='store_false')
    #FIXME: not only it ignores case differences, but it transforms it
    parser.add_argument('--to-lower', '-i', default=False, action='store_true',
        help='ignore lower/upper case differences')
    parser.add_argument('--linear_comp', help=PeripheralLinearBigramMatcher.__init__.__doc__)
    parser.add_argument('--format', default='{lemma}-{cat}', 
                        help="format used for the pivot")
    parser.add_argument('-dr', '--deprel', help='Dependency arc marching: specify the '
    'relation tag name')
    parser.add_argument('-dw','--depword', help='Dependency arc matching: left word regexp')
    parser.add_argument('-dp', '--deppos', help='Dependency arc matching: left pos regexp')
    parser.add_argument('-df', '--depfile', help='Dependency arc matching: file '
    'containing possible words of the right hand side')
    parser.add_argument('-hw', '--headword', help='Dependency arc matching: right word regexp')
    parser.add_argument('-hp', '--headpos', help='Dependency arc matching: right pos regexp')
    parser.add_argument('-hf', '--headfile', help='Dependency arc matching: file '
    'containing possible words of the right hand side')
    parser.add_argument('--no-color', help='don\'t print matches in color',
    action='store_true', default=False)

    args = parser.parse_args()

    match_funcs = build_matchers(args) 
    
    if not args.no_color:
        RED = '\033[91m'
        ENDC = '\033[0m'
    else:
        RED = ''
        ENDC = ''
    sentence = [] #list of tuples (w, l, pos, i, dep_i, dep_tag, "w-pos")
    plain_text_sentence = [] #list of lines read from parsed corpora
    for line in fileinput.input(args.corpora):
        if line.rstrip('\n') == "</s>":
            #detect compositions
            #comp_matches is a set, so we don't count repetitions
            comp_matches = set()
            for match_func in match_funcs:
                comp_matches.update(match_func.get_matches(sentence, plain_text_sentence))
            if comp_matches:
                print "<s>"
                #process sentence
                for i, t in enumerate(sentence): #i,t = index,tuple in sentence
                    has_match = any((t[3] - 1 ==i for match in comp_matches for t in match))
                    if has_match:
                        print RED + '\t'.join(str(x) for x in t[0:6]) + ENDC
                    else:
                        print '\t'.join(str(x) for x in t[0:6])
                print "</s>"
            #start a new sentence
            sentence = []
            plain_text_sentence = []
        elif line.startswith('<'):
            continue #skip
        else:
            line = line.rstrip('\n')
            t = line.split('\t')
            t[3] = int(t[3])
            t[4] = int(t[4])
            if args.to_lower:
                #FIXME: the lowecasign affects both the matching and the
                #output. Is this a bug or a feature?
                t[0] = t[0].lower()
                t[1] = t[1].lower()
            word = t[0]
            lem = t[1]
            #if args.pos:
            t.append(args.format.format(lemma=lem, word=word, cat=t[2][0].lower(), pos=t[2]))
            #FIXME: this makes no sense here, but need to add it for implementation issues 
            #(the matcher assumes the feature formatted string is there)
            t.append(args.format.format(lemma=lem, word=word, cat=t[2][0].lower(), pos=t[2]))
            sentence.append(tuple(t))   
            plain_text_sentence.append(line)


if __name__ == '__main__':
    main()
    
