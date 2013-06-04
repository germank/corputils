#!/usr/bin/env python

import argparse
import fileinput
import sys
from sentence_matchers import PeripheralDependencyBigramMatcher, PeripheralLinearBigramMatcher

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
    parser.add_argument('-d', '--disjoint', help='disjoint core and peripheral',
                        action='store_true')
    #parser.add_argument('-P', '--no-pos', help='don\'t add a POS indicator to '
    #                    'the lemmas', dest='pos', default=True, action='store_false')
    parser.add_argument('--to-lower', default=False, action='store_true',
        help='transform words and lemmas to lowercase')
    parser.add_argument('--linear_comp', help=PeripheralLinearBigramMatcher.__init__.__doc__)
    parser.add_argument('--lformat', default='{lemma}-{pos}')
    parser.add_argument('--rformat', default='{lemma}-{pos}')
    parser.add_argument('--lword', help='left composition word regexp')
    parser.add_argument('--lpos', help='left composition pos regexp')
    parser.add_argument('--lfile', help='file contining left composition words')
    parser.add_argument('--rword', help='right composition word regexp')
    parser.add_argument('--rpos', help='right composition pos regexp')
    parser.add_argument('--rfile', help='right contining left composition words')

    args = parser.parse_args()
    w = args.window_size
    #build functions that match a peripheral bigram
    match_funcs = []
    if args.linear_comp:
        match_funcs.append(PeripheralLinearBigramMatcher(args.linear_comp))
    
    if args.lword or args.lpos or args.lfile or args.rword or args.rpos or args.rfile:
        match_funcs.append(PeripheralDependencyBigramMatcher(args.lword, args.lpos, args.lfile, 
                 args.rword, args.rpos, args.rfile))
    
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
        if line.rstrip('\n') == "</s>":
            #detect compositions
            comp_matches = set()
            for match_func in match_funcs:
                comp_matches.update(match_func.get_matches(sentence, plain_text_sentence))
            
            #process sentence
            for i, t in enumerate(sentence): #i,t = index,tuple in sentence
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
            t.append(args.lformat.format(lemma=lem, word=word, pos=t[2][0].lower()))
            t.append(args.rformat.format(lemma=lem, word=word, pos=t[2][0].lower()))
            
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
