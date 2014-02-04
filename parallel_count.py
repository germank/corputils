#!/usr/bin/env python
import argparse
import fileinput
import sys
import logging
import os
import random
from clutils.config_loader import nodenames 
from sentence_matchers import *
from feature_extractor import *
from count_pipeline import StreamingCountPipeline, CountSumPipeline

def main():
    parser = argparse.ArgumentParser(description=
    '''Generates a list of coocurrence patterns of the form 
    pivot <direction> context
    given a dependency parsed corpus.
    Pivots = Context Words''')
    parser.add_argument('corpora', help='files with the parsed corpora',
        nargs='+')
    parser.add_argument('-D', '--debug', action='store_true', default=False,
    help="runs in local multithreading mode")
    parser.add_argument('--resume', action='store_true', default=False,
    help="If the output of a module is already present, don't re-run it "
    "(only useful if the job died)")
    parser.add_argument('-o', '--output', default='output', 
    help='output directory')
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
    parser.add_argument('-p', '--pivots', metavar='FILE', help='filter output '
    'pivots by those specified in the file (line-separated list of elements '
    'formatted as specified by -tf)')
    parser.add_argument('-t', '--targets', metavar='FILE', help='filter output '
    'targets by those specified in the file (line-separated list of elements '
    'formatted as specified by -tf)')
    parser.add_argument('-c', '--contexts', metavar='FILE', help='filter output '
    'context features by those specified in the file (line-separated list of elements '
    'formatted as specified by -cf)')
    parser.add_argument('-i', '--ignore_case', default=False, action='store_true',
        help='ignore case on match patterns')
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
    if args.pivots:
        pivot_words = set(w.strip() for w in file(args.pivots))
    else:
        pivot_words = None
    if args.contexts:
        contexts_words = set(w.strip() for w in file(args.contexts))
    else:
        contexts_words = None
    
    if args.targets:
        targets = set(w.strip() for w in file(args.targets))
    else:
        targets = None

    #FIXME: Matchers don't need to know target format (move filters to
    #TargetsFeaturesExtractor
    #create a matcher for the core space
    matchers = [UnigramMatcher(pivot_words, args.target_format)]
    #build functions that match a peripheral bigram
    matchers.extend(get_composition_matchers(args) )
    #FIXME: FeatureExtractors don't need to know target format (move filters to
    #TargetsFeaturesExtractor
    #define the kind of features we want to extract
    feature_extractor = BOWFeatureExtractor(args.window_size, contexts_words,
        args.context_format)
    #initialize extractor
    #FIXME: TargetsFeatureExtractor is not a FeatureExtractor (find a better name)
    targets_features_extractor = TargetsFeaturesExtractor(matchers,
                                                          feature_extractor,
                                                          args.target_format,
                                                          args.context_format,
                                                          targets)

    #FIXME: move to config.yml
    config = {
        '*': {
            'h_cpu': '24:0:0',
            'hosts': nodenames('compute-0-[1-9]|compute-1-[1-9]')
        },
        'count_matches': {
            'h_vmem': '7G'
        },
        'sum_matches': {
            'h_vmem': '120G'
        }
              
    }
    #pipeline = StreamingCountPipeline('compute-0-1', 17160,#random.randint(2000,32767), 
    #    os.path.join(os.getcwd(), args.output), targets_features_extractor, 
    #    args.corpora, args.gzip, args.target_format, args.context_format)
    pipeline = CountSumPipeline( 
        os.path.join(os.getcwd(), args.output), targets_features_extractor, 
        args.corpora, args.gzip, args.target_format, args.context_format,
        args.separator, args.to_lower)
    pipeline.run(debug=args.debug, resume=args.resume, config=config)

        
if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print "Aborted!"
        sys.exit(1)
