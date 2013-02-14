#!/usr/bin/env python

# window size
# sentence separator
# left composed pivot: regex(word) regex(POS) set<word>
# right composed pivot: regex(word) regex(POS) set<word>

import argparse
import fileinput
import re
import sys
from functools import partial


def main():
    parser = argparse.ArgumentParser(description=
    '''Generates a list of coocurrence patterns of the form 
    pivot <direction> context
    given a dependency parsed corpus.
    Pivots = Context Words''')
    parser.add_argument('corpora', help='files with the parsed corpora',
        default="-", nargs='?')
    parser.add_argument('-w', dest='window_size', type=int, default=None)
    parser.add_argument('-s', dest='separator', default='<s>')
    parser.add_argument('-x', '--comp_marker', default='<-->')
    parser.add_argument('--lword', help='left composition word regexp')
    parser.add_argument('--lpos', help='left composition pos regexp')
    parser.add_argument('--lfile', help='file contining left composition words')
    parser.add_argument('--rword', help='right composition word regexp')
    parser.add_argument('--rpos', help='right composition pos regexp')
    parser.add_argument('--rfile', help='right contining left composition words')

    args = parser.parse_args()
    w = args.window_size

    #caracteristic functions of the pair of words for which we are
    #interested in finding the composition coocurrence
    left_comp_match = build_composition_match_func(args.lword,
        args.lpos, args.lfile)
    right_comp_match = build_composition_match_func(args.rword,
        args.rpos, args.rfile)

    match_tag = re.compile("</?s>|</?text.*?>").match

    sentence = [] #list of tuples (w, l, pos, i, dep_i, dep_tag, "w-pos")
    i=0
    for line in fileinput.input(args.corpora):
        i+=1
        if i % 1000 == 0:
            sys.stderr.write('.')
            if i % 80000 == 0:
                sys.stderr.write('\n')
        if line.rstrip('\n') == "</s>":
            #process sentence
            for i, t in enumerate(sentence): #i,t = index,tuple in sentence
                #print coocurrences
                lend = max(0,i-w) if w else 0
                for lt in sentence[lend:i]:
                    print "{0}\tl\t{1}".format(t[-1], lt[-1])
                rend = min(len(sentence),i+(w+1)) if w else len(sentence)
                for rt in sentence[i+1:rend]:
                    print "{0}\tr\t{1}".format(t[-1], rt[-1])
                #check if t should be composed
                if left_comp_match(t) and t[4] > 0:
                    comp_t = sentence[t[4]-1]
                    assert comp_t[3] == t[4]
                    if right_comp_match(comp_t):
                        comp_pivot = "{0}{1}{2}".format(
                            t[-1], args.comp_marker, comp_t[-1])
                        #put the composed words in order
                        if int(t[3]) < int(comp_t[3]):
                            lcomp_t = t
                            rcomp_t = comp_t
                        else:
                            lcomp_t = comp_t
                            rcomp_t = t
                        l = int(lcomp_t[3])-1
                        r = int(rcomp_t[3])-1
                        #print coocurrences
                        #count left of first composed word
                        lend = max(0,l-w) if w else 0
                        for lt in sentence[lend:l]:
                            print "{0}\tl\t{1}".format(comp_pivot, lt[-1])
                        #count right of first and left of second in range of
                        #the first
                        lmid = min(r,l+(w+1)) if w else r
                        for ct in sentence[l+1:lmid]:
                            print "{0}\tc\t{1}".format(comp_pivot, ct[-1])
                        #count right of first and left of second in range of
                        #the second
                        rmid = max(lmid,r-w) if w else r
                        for ct in sentence[rmid:r]:
                            print "{0}\tc\t{1}".format(comp_pivot, ct[-1])
                        rend = min(len(sentence), r+(w+1)) if w else \
                            len(sentence)
                        for rt in sentence[r+1:rend]:
                            print "{0}\tr\t{1}".format(comp_pivot, rt[-1])

            #start a new sentence
            sentence = []
        elif match_tag(line):
            continue #skip
        else:
            t = line.rstrip('\n').split('\t')
            t[3] = int(t[3])
            t[4] = int(t[4])
            #append pos tag as the first letter in lowercase
            t.append("{0}-{1}".format(t[1].lower(),t[2][0].lower()))
            sentence.append(t)   

def load_words(filename):
    pivots = set()
    for line in fileinput.input(filename):
        pivots.add(line.strip(' \t\n'))
    return pivots

def build_composition_match_func(word_regexp, pos_regexp, wordset_file):
    '''
    Returns a function that selects tokens in a DP.
    The criteria for the selection can be determined according to a regexp,
    a POS tag, or a file containing a set of lemmas
    '''
    #I try to build the function using the least possible amount of lambdas
    #Still, they are quite a lot
    match_func = None
    if word_regexp:
        word_regexp_func = re.compile(word_regexp, re.I).match
        if match_func:
            match_func = partial(lambda f, w: f(w) and word_regexp_func(w[1]),
                match_func)
        else:
            match_func = lambda w: word_regexp_func(w[1])
    if pos_regexp:
        pos_regexp_func = re.compile(pos_regexp, re.I).match
        if match_func:
            match_func = partial(lambda f, w: f(w) and pos_regexp_func(w[2]),
                match_func)
        else:
            match_func = lambda w: pos_regexp_func(w[2])
    if wordset_file:
        wordset = load_words(wordset_file)
        in_wordset = wordset.__contains__
        if match_func:
            match_func = partial(lambda f, w: f(w) and in_wordset(w[1]),
                match_func)
        else:
            match_func = lambda w: in_wordset(w[1])

    if match_func:
        return match_func
    else:
        return lambda w: False


        
if __name__ == '__main__':
    main()
