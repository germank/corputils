#!/usr/bin/env python

import argparse
import fileinput
import sys
from sentence_matchers import *

def main():
    parser = argparse.ArgumentParser(description=
    '''Generates a list of coocurrence patterns of the form 
    pivot <direction> context
    given a dependency parsed corpus.
    Pivots = Context Words''')
    parser.add_argument('corpora', help='files with the parsed corpora',
        default="-", nargs='?')
    parser.add_argument('-w', dest='window_size', type=int, default=None)
    parser.add_argument('-s', dest='separator', default='s', help="sentence "
    "separator (default=s)")
    parser.add_argument('-x', '--comp_marker', default='<-->', help="token "
    "separator for composed bigrams (e.g. red-j<-->car-n)")
    parser.add_argument('-d', '--disjoint', help='disjoint context for core and peripheral',
                        action='store_true')
    parser.add_argument('-c', '--core', help='specify the file containing the words in the core space')
    parser.add_argument('--to-lower', default=False, action='store_true',
        help='transform words and lemmas to lowercase')
    parser.add_argument('-tf', '--target-format', default='{lemma}-{cat}', 
                        help="format used for the target. Variables are "
                        "{word}, {lemma}, {pos} and {cat}")
    parser.add_argument('-cf', '--context-format', default='{lemma}-{cat}', 
                        help="format used for the context. Variables are "
                        "{word}, {lemma}, {pos} and {cat}")
    parser.add_argument('--linear_comp', help=PeripheralLinearBigramMatcher.__doc__)
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

    args = parser.parse_args()
    w = args.window_size
    #build functions that match a peripheral bigram
    match_funcs = build_matchers(args) 

    if args.core:
        core_words = set(w.strip() for w in file(args.core))
    else:
        core_words = None
    sentence = [] #list of tuples (w, l, pos, i, dep_i, dep_tag, "w-pos")
    plain_text_sentence = [] #list of lines read from parsed corpora
    i=0
    for line in fileinput.input(args.corpora):
        #FIXME: abstract into a progress bar
        i+=1
        if i % 1000 == 0:
            sys.stderr.write('.')
            if i % 80000 == 0:
                sys.stderr.write('\n')
        if line.strip('\n<>') == "/{0}".format(args.separator):
            #detect compositions
            #comp_matches is a set, so we don't count repetitions
            comp_matches = set()
            for match_func in match_funcs:
                for m in match_func.get_matches(sentence, plain_text_sentence):
                    #we only target bigrams
                    if len(m)>2:
                        m = (m[0], m[-1])
                    elif len(m)<2:
                        continue
                    comp_matches.add(m)
            
            #process sentence
            for i, t in enumerate(sentence): #i,t = index,tuple in sentence
                #it doesn't print pivot coocurrences if core has been specified
                #and the pivot is not there
                if core_words and t[-2] not in core_words:
                    continue
                #it doesn't print pivot coocurrences if args.disjoint is
                #specified and this is context for composition
                if not args.disjoint or not is_target_composition(t, comp_matches):
                    #print coocurrences
                    lend = max(0,i-w) if w else 0
                    for lt in sentence[lend:i]:
                        print "{0}\tl\t{1}".format(t[-2], lt[-1])
                    rend = min(len(sentence),i+(w+1)) if w else len(sentence)
                    for rt in sentence[i+1:rend]:
                        print "{0}\tr\t{1}".format(t[-2], rt[-1])
                    #check if t should be composed
                
            for t,comp_t in comp_matches:
                    #it doesn't print peripheral coocurrences if core has been specified
                    #and the target is not there
                    if core_words and comp_t[-2] not in core_words:
                        continue
                    comp_pivot = "{0}{1}{2}".format(
                        t[-2], args.comp_marker, comp_t[-2])
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
                    #and print context as "l" (left)
                    lend = max(0,l-w) if w else 0
                    for lt in sentence[lend:l]:
                        print "{0}\tl\t{1}".format(comp_pivot, lt[-1])
                    #count right of first and left of second in range of
                    #the first
                    #and print context as "c" (center)
                    #FIXME: is this even useful?
                    lmid = min(r,l+(w+1)) if w else r
                    for ct in sentence[l+1:lmid]:
                        print "{0}\tc\t{1}".format(comp_pivot, ct[-1])
                    #count right of first and left of second in range of
                    #the second
                    #and print context as "r" (right)
                    rmid = max(lmid,r-w) if w else r
                    for ct in sentence[rmid:r]:
                        print "{0}\tc\t{1}".format(comp_pivot, ct[-1])
                    rend = min(len(sentence), r+(w+1)) if w else \
                        len(sentence)
                    for rt in sentence[r+1:rend]:
                        print "{0}\tr\t{1}".format(comp_pivot, rt[-1])

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
            t.append(args.target_format.format(lemma=lem, word=word, cat=t[2][0].lower(), pos=t[2]))
            t.append(args.context_format.format(lemma=lem, word=word, cat=t[2][0].lower(), pos=t[2]))
            
            sentence.append(tuple(t))   
            plain_text_sentence.append(line)


#auxiliary functions
def is_target_composition(t1, comp_matches):
    for _,t in comp_matches:
        if t == t1:
            return True
    return False
        

        
if __name__ == '__main__':
    main()
