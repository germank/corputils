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
from itertools import repeat

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

def load_words(filename):
    pivots = set()
    for line in fileinput.input(filename):
        pivots.add(line.strip(' \t\n'))
    return pivots



class PeripheralLinearBigramMatcher():
    def __init__(self, linear_comp):
        '''linear_comp: A pseudo-regular expression to match 
        composition phrases bases on linear order.
        Each token is represented with a T<> marker which can 
        take as optional arguments "word" and "pos". 
        E.g. T<word=big,pos=JJ>(T<pos=JJ>)*T<pos=NN>'''
        
        if not linear_comp:
            self.linear_comp_match = None
        def token_expr(expr):
            ret_exprs = list(repeat(r"[^\t\|]*?", 6))
            for _, kw, value in re.findall(r'(([^,=]+)=([^,=]+))', expr):
                if kw == 'pos':
                    ret_exprs[2] = "({0})".format(value)
                if kw == 'word':
                    ret_exprs[1] = "({0})".format(value)
            return '\\t'.join(ret_exprs)
        expr = re.sub('T<(.*?)>', lambda m:r"\|({0})".format(token_expr(m.group(1))), linear_comp) + r"\|"
        self.linear_comp_match = re.compile(expr)
    
    def get_matches(self, sentence, plain_text_sentence):
        ret = []
        if self.linear_comp_match:
            #we transform the sentence into a string
            sentence_str = "|{0}|".format("|".join(plain_text_sentence))
            token_lens = [len(t)+1 for t in plain_text_sentence]
            partial_sums = [sum(token_lens[:i]) for i in range(len(token_lens)+1)]
            #token_lims = [(s,e-1) for s,e in zip(partial_sums[:-1], partial_sums[1:])] 
            token_start2pos = {s:i for i,s in enumerate(partial_sums[:-1])}
            token_end2pos = {e:i for i,e in enumerate(partial_sums[1:])}
            #for each match of the pseudo-regexp in the sentence
            for i,m in enumerate(self.linear_comp_match.finditer(sentence_str)):
                #obtain the matched tokens
                left_match_pos = token_start2pos[m.start(0)]
                right_match_pos = token_end2pos[m.end(0)-1]
                ret.append((sentence[left_match_pos], 
                            sentence[right_match_pos]))
        return ret

class PeripheralDependencyBigramMatcher():
    def __init__(self, lword, lpos, lfile, rword, rpos, rfile):
        '''Matches bigrams across dependency arcs'''
        self.left_comp_match = self._build_composition_match_func(lword, lpos, lfile)
        self.right_comp_match = self._build_composition_match_func(rword, rpos, rfile)
    
    def composition_target(self,  t, sentence):
        '''If t is a matching left node, then return
        the node which is dependent upon'''
        if self.left_comp_match(t) and t[4] > 0:
            comp_t = sentence[t[4]-1]
            assert comp_t[3] == t[4]
            if self.right_comp_match(comp_t):
                return comp_t
        return None

    def get_matches(self, sentence, _):
        comp_matches = []
        for i, t in enumerate(sentence): #i,t = index,tuple in sentence
            comp_t = self.composition_target(t, sentence)
            if comp_t:
                comp_matches.append((t,comp_t))
        return comp_matches

    def _build_composition_match_func(self, word_regexp, pos_regexp, wordset_file):
        '''
        Returns a function that selects tokens in a DP.
        The criteria for the selection can be determined according to a regexp,
        a POS tag, or a file containing a set of lemmas
        '''
        #I've tried to build the function using the least possible amount of lambdas
        #Still, there are quite a lot
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
                #FIXME: by using the index -2 we are referring to the formatted
                #element. Bug or feature?
                match_func = partial(lambda f, w: f(w) and in_wordset(w[-2]),
                    match_func)
            else:
                match_func = lambda w: in_wordset(w[-2])
    
        if match_func:
            return match_func
        else:
            return lambda w: True

        

        
if __name__ == '__main__':
    main()
