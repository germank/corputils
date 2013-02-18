#!/usr/bin/env python
import argparse
import fileinput
import re
import os

def main():
    parser = argparse.ArgumentParser(description=
    '''Removes non-pivots words from DP Corpora
    ''')
    parser.add_argument('corpora', help='files with the parsed corpora',
        default="-", nargs='?')
    parser.add_argument('--to-lower', default=False, 
        help='changes lemmas to lowecase', action='store_true')
    parser.add_argument('--pword', help='pivot pos regexp')
    parser.add_argument('--ppos', help='pivot pos regexp')
    parser.add_argument('--pwordset', help='file with a list of words that '
    'should be kept')

    args = parser.parse_args()

    is_pivot = get_pivot_filter(args)
    match_tag = re.compile("</?s>|</?text.*?>").match

    sentence = [] #list of tuples (w, l, pos, i, dep_i, dep_tag, "w-pos")
    for line in fileinput.input(args.corpora):
        line = line.rstrip('\n')
        if line == "</s>":
            sentence = keep_pivots(sentence, is_pivot)
            for t in sentence:
                print "\t".join((str(x) for x in t))
            print line
            sentence = []
        elif match_tag(line):
            #omit <s></s><text></text>
            print line
        else:
            t = line.split('\t')
            t[3] = int(t[3])
            t[4] = int(t[4])
            if args.to_lower:
                t[1] = t[1].lower()
            #append pos tag as the first letter in lowercase
            sentence.append(t)   

def get_pivot_filter(args):
    #pick up pivots
    if args.pwordset:
        pivots = load_words(args.pwordset)
        is_pivot = lambda w: w[1] in pivots
    elif args.ppos or args.pwords:
        if args.ppos and args.pwords:
            word_match = re.compile(args.pwords, re.IGNORECASE).match
            pos_match = re.compile(args.ppos, re.IGNORECASE).match
            is_pivot = lambda w: word_match(w[1]) and pos_match(w[2])
        elif args.ppos:
            pos_match = re.compile(args.ppos, re.IGNORECASE).match
            is_pivot = lambda w: pos_match(w[2])
        else:
            word_match = re.compile(args.pwords, re.IGNORECASE).match
            is_pivot = lambda w: word_match(w[1])
            
    else:
        is_pivot = lambda w: True
    return is_pivot

def load_words(filename):
    pivots = set()
    for line in fileinput.input(filename):
        pivots.add(line.strip(' \t\n'))
    return pivots

def keep_pivots(sentence, is_pivot):
    '''removes the non-pivot words from the sentence and shift left the 
    dependency and word indexes in order to keep the references consistent'''
    #set the pivots we want to delete
    filtered_pos = []
    for t in sentence:
        if not is_pivot(t):
            filtered_pos.append(t[3])
            
    filtered_sentence = []
    for t in sentence:
        if is_pivot(t):
            i_offset = 0
            dep_offset = 0
            dangling = False
            for p in filtered_pos:
                if t[3] > p:
                    i_offset += 1
                if t[4] > p:
                    dep_offset += 1
                elif t[4] == p:
                    dangling = True
            t[3] -= i_offset
            if dangling:
                t[4] = -1
            else:
                t[4] -= dep_offset
            filtered_sentence.append(t)
            assert all([t[4] <= len(sentence) for t in sentence])
    return filtered_sentence



if __name__ == '__main__':
    main()
