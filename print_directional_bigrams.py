#!/usr/bin/env python

import argparse
import sys
import logging
import itertools
import fileinput
from corputils.core.readers import DPCorpusReader
from corputils.core.aux import gziplines
logging.basicConfig(level=logging.INFO)

from corputils.core.sentence_matchers import UnigramMatcher,\
    get_composition_matchers
from corputils.core.feature_extractor import BOWFeatureExtractor, TargetsFeaturesExtractor

def main():
    parser = argparse.ArgumentParser(description=
    '''Generates a list of coocurrence patterns of the form 
    pivot <direction> context
    given a dependency parsed corpus.
    Pivots = Context Words''')
    parser.add_argument('corpora', help='files with the parsed corpora',
        default="-", nargs='*')
    parser.add_argument('-z', '--gzip', action='store_true', default=False, 
    help="Interpret corpora as gzipped files")
    parser.add_argument('-w', dest='window_size', type=int, default=None)
    parser.add_argument('-s', dest='separator', default='s', help="sentence "
    "separator (default=s)")
    parser.add_argument('-x', '--token_sep', default='<-->', help="token "
    "separator for composed bigrams (e.g. red-j<-->car-n)")
    #temporarly disabled
    #parser.add_argument('-d', '--disjoint', help='disjoint context for core and peripheral',
    #                    action='store_true')
    parser.add_argument('-t0', '--targets0', metavar='FILE', help='filter output '
    'unigram targets for which the lexical item is not in the provided list '
    '(line-separated list of elements formatted as specified by -tf)')
    parser.add_argument('-t1', '--targets1', metavar='FILE', help='filter output '
    'bigram targets for which the 1st lexical item is not in the provided list '
    '(line-separated list of elements formatted as specified by -tf)')
    parser.add_argument('-t2', '--targets2', metavar='FILE', help='filter output '
    'bigram targets for which the 1st lexical item is not in the provided list '
    '(line-separated list of elements formatted as specified by -tf)')
    parser.add_argument('-c', '--contexts', metavar='FILE', help='filter output '
    'context features by those specified in the file (line-separated list of elements '
    'formatted as specified by -cf)')
    parser.add_argument('-i', '--ignore_case', default=False, action='store_true',
        help='ignore case on match patterns')
    #DISABLED
    parser.add_argument('--to-lower', default=False, action='store_true',
        help='transform words and lemmas to lowercase')
    parser.add_argument('-tf', '--target-format', default='{lemma}-{cat}', 
                        help="format used for the target. Variables are "
                        "{word}, {lemma}, {pos} and {cat}")
    parser.add_argument('-cf', '--context-format', default='{lemma}-{cat}', 
                        help="format used for the context. Variables are "
                        "{word}, {lemma}, {pos} and {cat}")
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
    w = args.window_size
    
    targets = {}
    #Target unigrams filter
    targets[1] = {}
    if args.targets0:
        targets[1][1] = set(w.strip() for w in file(args.targets0))

    

    targets[2] = {}
    if args.targets1:
        targets[2][1] = set(w.strip() for w in file(args.targets1))
        
    if args.targets2:
        targets[2][2] = set(w.strip() for w in file(args.targets2))
        
    if args.contexts:
        contexts_words = set(w.strip() for w in file(args.contexts))
    else:
        contexts_words = None
        
    #create a matcher for the core space
    matchers = [UnigramMatcher(None, args.target_format)]
    #build functions that match a peripheral bigram
    matchers.extend(get_composition_matchers(args) )
    #define the kind of features we want to extract
    feature_extractor = BOWFeatureExtractor(args.window_size, contexts_words,
        args.context_format)
    #initialize extractor
    targets_features_extractor = TargetsFeaturesExtractor(matchers,
                                                          feature_extractor,
                                                          args.target_format,
                                                          args.context_format,
                                                          targets)
    #open corpus
    if args.gzip:
        input_corpora = itertools.chain(*map(gziplines, args.corpora))
    else:
        input_corpora = fileinput.FileInput(args.corpora)
        
    corpus_reader = DPCorpusReader(input_corpora,
                                   separator=args.separator,
                                   to_lower=args.to_lower)

    targets_features_extractor.initialize()
    #print directional bigrams
    for target, feature in targets_features_extractor(corpus_reader):
        print "{0}\t{1}".format(target.format(args.target_format), 
                                feature.format(args.context_format))

        

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
