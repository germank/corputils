import logging
import os
from clutils import JobModule, Pipeline, PinMultiplex, DictionaryPin, TextFilePin
from readers import DPCorpusReader
from collections import Counter
from clutils.serialization import TxtSerializer

class CountMatches(JobModule):
    def setup(self):
        self.register_pins(TextFilePin('corpus'),
                   DictionaryPin('output', Counter))

    def run(self, targets_features_extractor, corpus_file, gzip, 
        target_format, context_format, sentence_separator,
                  to_lower):
        targets_features_extractor.initialize()
        self['corpus'].open(corpus_file, gzip)
        logging.info("CountMatches({0}): starting "
        "counting".format(self))
        
        corpus_reader = DPCorpusReader(self['corpus'].read(), 
                                       separator=sentence_separator,
                                       to_lower=to_lower)
        for i,(target, feature) in \
            enumerate(targets_features_extractor(corpus_reader)):
            if (i+1) % 100000 == 0:
                logging.info("CountMatches({0}): {1} features extracted "
                             "so far...".format(self, i+1)) 
            self['output'][(target.format(target_format), 
                feature.format(context_format))] += 1
        logging.info("CountMatches: finished")

    def finished(self):
        return self['output'].file_exists()
        


class SumResults(JobModule):
    def setup(self):
        self.register_pins(PinMultiplex('counts'),
            DictionaryPin('output', Counter, TxtSerializer))
    
    def run(self):
        logging.info("SumMatches: starting")
        for cnt in self['counts']:
            logging.info('integrating pin')
            for k,v in cnt.iteritems():
                fk = "\t".join(k)
                self['output'][fk] += v
            logging.info('integration done')
        logging.info("SumMatches: finished")

    def finished(self):
        return self['output'].file_exists()

class CountSumPipeline(Pipeline):
    def __init__(self, work_path, targets_features_extractor, corpora, gzip,
    target_format, context_format, sentence_separator, to_lower):
        super(CountSumPipeline, self).__init__(work_path)
        sum_module = SumResults('sum_matches')
        count_modules = []
        for corpus in corpora:
            count_module = CountMatches('count_matches', os.path.basename(corpus))
            count_module.set_args(targets_features_extractor, corpus, gzip,
                                  target_format, context_format, 
                                  sentence_separator, to_lower)
            
            count_module['output'].connect_to(sum_module['counts'])
            count_modules.append(count_module)
        self.add_stage(*count_modules)
        self.add_stage(sum_module)

