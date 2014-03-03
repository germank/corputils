import logging
import os
from clutils import JobModule, Pipeline, PinMultiplex, DictionaryPin, TextFilePin
from readers import DPCorpusReader
from collections import Counter

class CountMatches(JobModule):
    def __init__(self, work_path, targets_features_extractor, corpus_file, gzip, 
                 target_format, context_format, sentence_separator, sentence_filter,
                  to_lower):
        super(CountMatches, self).__init__(os.path.join(work_path, 
            os.path.basename(corpus_file)))
        self.targets_features_extractor = targets_features_extractor
        self.target_format = target_format
        self.context_format = context_format
        self.sentence_separator = sentence_separator
        self.sentence_filter = sentence_filter
        self.to_lower = to_lower
        
        self.setup(TextFilePin(self, 'corpus', corpus_file, gzip),
                   DictionaryPin(self, 'output', Counter))

    def run(self):
        logging.getLogger().setLevel(logging.INFO)#FIXME: move away
        logging.info("CountMatches({0}): starting "
        "counting".format(self.work_path))
        
        corpus_reader = DPCorpusReader(self['corpus'].read(), 
                                       sentence_filter=self.sentence_filter,
                                       separator=self.sentence_separator,
                                       to_lower=self.to_lower)
        for i,(target, feature) in \
            enumerate(self.targets_features_extractor(corpus_reader)):
            if (i+1) % 100000 == 0:
                logging.info("CountMatches({0}): {1} features extracted "
                             "so far...".format(self.work_path, i+1)) 
            self['output'][(target.format(self.target_format), 
                feature.format(self.context_format))] += 1
        logging.info("CountMatches: finished")

    def finished(self):
        return self['output'].file_exists()
        


class SumResults(JobModule):
    def __init__(self, work_path):
        super(SumResults, self).__init__(work_path)
        self.setup(PinMultiplex(self, 'counts'),
            DictionaryPin(self, 'output'))
    
    def run(self):
        logging.info("SumMatches: loading words")
        words = set(l.strip() for l in file('freq_more_50_words.txt'))
        words = set.intersection(words, set(l.strip() for l in file('cw.rows')))
        logging.info("SumMatches: starting")
        ret = {}
        for cnt in self['counts']:
            logging.info('integrating pin')
            for k,v in cnt.iteritems():
                if '<-->' in k[0]:
                    m,n = k[0].split('<-->')
                    if n not in words:
                        continue
                fk = "\t".join(k)
                if fk not in ret:
                    ret[fk] = 0
                ret[fk] += v
            logging.info('integration done')
        logging.info("SumMatches: saving results")
        self['output'].write(ret)
        logging.info("SumMatches: finished")

    def finished(self):
        return self['output'].file_exists()

class CountSumPipeline(Pipeline):
    def __init__(self, work_path, targets_features_extractor, corpora, gzip,
    target_format, context_format, sentence_separator, sentence_filter, to_lower):
        super(CountSumPipeline, self).__init__(work_path)
        sum_module = SumResults(os.path.join(work_path, 'sum_matches'))
        count_modules = []
        for corpus in corpora:
            count_module = CountMatches(os.path.join(work_path,
                'count_matches'), targets_features_extractor, corpus, gzip,
                target_format, context_format, sentence_separator, sentence_filter, to_lower)
            
            sum_module['counts'].append(count_module['output'])
            #FIXME: change for equivalent
            #count_module['output'].connect_to(sum_module['counts'])
            count_modules.append(count_module)
        self.add_stage(*count_modules)
        self.add_stage(sum_module)

