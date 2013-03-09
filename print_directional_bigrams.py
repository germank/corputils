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

#global variables: i promise not to do it again!
left_comp_match = None
right_comp_match = None


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
    parser.add_argument('--to-lower', default=False, action='store_true',
        help='transform lemmas to lowercase')
    parser.add_argument('--linear_comp', help='A pseudo-regular expression to match '
                        'composition phrases bases on linear order.'
                        'Each token is represented with a T<> marker which can '
                        'take as optional arguments "word" and "pos". '
                        'E.g. T<word=big,pos=jj>(T<pos=jj>)*T<pos=nn>')
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
    if args.linear_comp:
        linear_comp_match = build_linear_match_func(args.linear_comp)
    else:
        linear_comp_match = None
    left_comp_match = build_composition_match_func(args.lword,
        args.lpos, args.lfile)
    right_comp_match = build_composition_match_func(args.rword,
        args.rpos, args.rfile)

    match_tag = re.compile("</?s>|</?text.*?>").match

    sentence = [] #list of tuples (w, l, pos, i, dep_i, dep_tag, "w-pos")
    tokens_str = []
    i=0
    for line in fileinput.input(args.corpora):
        i+=1
        if i % 1000 == 0:
            sys.stderr.write('.')
            if i % 80000 == 0:
                sys.stderr.write('\n')
        if line.rstrip('\n') == "</s>":
            #detect compositions
            comp_matches = set()
            if linear_comp_match:
                comp_matches.update(linear_composition_matches(linear_comp_match, sentence, tokens_str))
            for i, t in enumerate(sentence): #i,t = index,tuple in sentence
                comp_t = composition_target(left_comp_match, right_comp_match,
                                            t, sentence)
                if comp_t:
                    comp_matches.add((t,comp_t))
            #process sentence
            for i, t in enumerate(sentence): #i,t = index,tuple in sentence
                #it doesn't print pivot coocurrences if args.disjoint is
                #specified and this is context for composition
                if not args.disjoint or not is_target_composition(t, comp_matches):
                    #print coocurrences
                    lend = max(0,i-w) if w else 0
                    for lt in sentence[lend:i]:
                        print "{0}\tl\t{1}".format(t[-1], lt[-1])
                    rend = min(len(sentence),i+(w+1)) if w else len(sentence)
                    for rt in sentence[i+1:rend]:
                        print "{0}\tr\t{1}".format(t[-1], rt[-1])
                    #check if t should be composed
                
            for t,comp_t in comp_matches:
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
            tokens_str = []
        elif match_tag(line):
            continue #skip
        else:
            line = line.rstrip('\n')
            t = line.split('\t')
            t[3] = int(t[3])
            t[4] = int(t[4])
            #append pos tag as the first letter in lowercase
            if args.to_lower:
                t.append("{0}-{1}".format(t[1].lower(),t[2][0].lower()))
            else:
                t.append("{0}-{1}".format(t[1],t[2][0].lower()))
            sentence.append(tuple(t))   
            tokens_str.append(line)

def is_target_composition(t1, comp_matches):
    for _,t in comp_matches:
        if t == t1:
            return True
    return False

def linear_composition_matches(linear_comp_match, sentence, tokens_str):
    ret = []
    if linear_comp_match:
        #we transform the sentence into a string
        sentence_str = "|{0}|".format("|".join(tokens_str))
        token_lens = [len(t)+1 for t in tokens_str]
        partial_sums = [sum(token_lens[:i]) for i in range(len(token_lens)+1)]
        #token_lims = [(s,e-1) for s,e in zip(partial_sums[:-1], partial_sums[1:])] 
        token_start2pos = {s:i for i,s in enumerate(partial_sums[:-1])}
        token_end2pos = {e:i for i,e in enumerate(partial_sums[1:])}
        #for each match of the pseudo-regexp in the sentence
        for i,m in enumerate(linear_comp_match.finditer(sentence_str)):
            #obtain the matched tokens
            left_match_pos = token_start2pos[m.start(0)]
            right_match_pos = token_end2pos[m.end(0)-1]
            ret.append((sentence[left_match_pos], 
                        sentence[right_match_pos]))
    return ret
 
def composition_target(left_comp_match, right_comp_match, t, sentence):
    if left_comp_match(t) and t[4] > 0:
        comp_t = sentence[t[4]-1]
        assert comp_t[3] == t[4]
        if right_comp_match(comp_t):
            return comp_t
    return None

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


def build_linear_match_func(linear_comp):
    def token_expr(expr):
        ret_exprs = list(repeat(r"[^\t\|]*?", 6))
        for _, kw, value in re.findall(r'(([^,=]+)=([^,=]+))', expr):
            if kw == 'pos':
                ret_exprs[2] = "({0})".format(value)
            if kw == 'word':
                ret_exprs[1] = "({0})".format(value)
        return '\\t'.join(ret_exprs)
    expr = re.sub('T<(.*?)>', lambda m:r"\|({0})".format(token_expr(m.group(1))), linear_comp) + r"\|"
    return re.compile(expr)

        
if __name__ == '__main__':
    main()
