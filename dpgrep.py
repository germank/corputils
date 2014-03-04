#!/usr/bin/env python
'''
Created on Jun 12, 2013

@author: german
'''
import argparse
import fileinput

from corputils.core.sentence_matchers import get_composition_matchers
from corputils.core.readers import DPCorpusReader
from corputils.core.aux import gziplines
import itertools
import sys

def main():
    parser = argparse.ArgumentParser(description=
    '''Prints sentences that match the given cirteria''')
    parser.add_argument('corpora', help='files with the parsed corpora',
        default="-", nargs='*')
    parser.add_argument('-z', '--gzip', action='store_true', default=False, 
    help="Interpret corpora as gzipped files")
    parser.add_argument('-s', dest='separator', default='s', help="sentence "
    "separator (default=s)")
    parser.add_argument('-x', '--token_sep', default='<-->', help="token "
    "separator for composed bigrams (e.g. red-j<-->car-n)")
    parser.add_argument('-i', '--ignore_case', default=False, action='store_true',
        help='ignore case on match patterns')
    parser.add_argument('--to-lower', default=False, action='store_true',
        help='transform words and lemmas to lowercase')
    parser.add_argument('-tf', '--target-format', default='{lemma}-{cat}', 
                        help="format used for the target. Variables are "
                        "{word}, {lemma}, {pos} and {cat}")
    parser.add_argument('--no-color', help='don\'t print matches in color',
    action='store_true', default=False)
    parser.add_argument('--linear_comp', help='''Match phrases based on a pseudo-regular expression.
    Each token is represented with a T<> marker which can 
    take as optional arguments "word" and "pos". 
    E.g. T<word=big,pos=JJ>(T<pos=JJ>)*T<word=file(rows.txt),pos=NN|NNS>''')
    parser.add_argument('-dr', '--deprel', help='Dependency arc marching: specify the '
    'relation tag name')
    parser.add_argument('-dw','--depword', help='Dependency arc matching: left word regexp')
    parser.add_argument('-dl','--deplemma', help='Dependency arc matching: left '
    'lemma regexp')
    parser.add_argument('-dp', '--deppos', help='Dependency arc matching: left pos regexp')
    parser.add_argument('-df', '--depfile', help='Dependency arc matching: file '
    'containing possible dependent tokens (with the format specified by -tf)')
    parser.add_argument('-hl', '--headlemma', help='Dependency arc matching: right '
    'lemma regexp')
    parser.add_argument('-hw', '--headword', help='Dependency arc matching: right word regexp')
    parser.add_argument('-hp', '--headpos', help='Dependency arc matching: right pos regexp')
    parser.add_argument('-hf', '--headfile', help='Dependency arc matching: file '
    'containing possible head tokens (with the format specified by -ff)')

    args = parser.parse_args()

    match_funcs = get_composition_matchers(args)
    if args.gzip:
        input_corpora = itertools.chain(*map(gziplines, args.corpora))
    else:
        input_corpora = fileinput.FileInput(args.corpora)
    
    corpus_reader = DPCorpusReader(input_corpora,
                                   separator=args.separator,
                                   to_lower=args.to_lower) 
    
    if not args.no_color:
        RED = '\033[91m'
        ENDC = '\033[0m'
    else:
        RED = ''
        ENDC = ''
        
    corp_format = "\t".join(map(lambda x: "{"+x+"}", corpus_reader.corp_format))
    for sentence in corpus_reader:
        #detect compositions
        #comp_matches is a set, so we don't count repetitions
        comp_matches = set()
        for match_func in match_funcs:
            comp_matches.update(match_func.get_matches(sentence))
        if comp_matches:
            print "<s>"
            #process sentence
            for s_i in sentence: #i,t = index,tuple in sentence
                has_match = any((t ==s_i for match in comp_matches for t in match))
                if has_match:
                    print RED + s_i.format(corp_format) + ENDC
                else:
                    print s_i.format(corp_format)
            print "</s>"



if __name__ == '__main__':
    try:
        main()
    except IOError, e:
        if e.errno == 32:
            #broken pipe, do nothing
            pass
    except KeyboardInterrupt:
        print >>sys.stderr, 'Aborting!'
        sys.exit(1)    
