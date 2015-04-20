import logging
logging.basicConfig(level=logging.DEBUG)
import os
from clutils import JobModule, Pipeline, PinMultiplex, DictionaryPin, TextFilePin
from readers import DPCorpusReader
from collections import Counter
from clutils.serialization import TxtSerializer
#try:
from clutils.serialization import Hdf5Serializer
DefaultSerializer = Hdf5Serializer
#except:
#    from clutils.serialization import PklSerializer
#    DefaultSerializer = PklSerializer

def memory_usage():
    """Memory usage of the current process in kilobytes."""
    status = None
    result = {'peak': 0, 'rss': 0}
    try:
        # This will only work on systems with a /proc file system
        # (like Linux).
        status = open('/proc/self/status')
        for line in status:
            parts = line.split()
            key = parts[0][2:-1].lower()
            if key in result:
                result[key] = int(parts[1])
    finally:
        if status is not None:
            status.close()
    return result

class CountMatches(JobModule):
    def setup(self):
        self.register_pins(TextFilePin('corpus'),
                   DictionaryPin('output', Counter, 
                                 serializer_type=DefaultSerializer))

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
                logging.info("CountMatches({0}): {1} MB used (peak) "\
                                .format(self, memory_usage()['peak']/1024))
            #print target.format(target_format),\
            enc_target = targets_features_extractor.encode_target(target)
            enc_context = targets_features_extractor.encode_feature(feature)
            fmt_target = target.format(target_format)#342-344
            fmt_context = feature.format(context_format)
            self['output'][(enc_target, 
                enc_context)] += 1
                #feature.format(context_format))] += 1
        logging.info("CountMatches: finished")

    def finished(self):
        return self['output'].file_exists()
        


class SumResults(JobModule):
    def setup(self):
        self.register_pins(PinMultiplex('counts'),
            DictionaryPin('output', Counter, TxtSerializer))
    
    def run(self, targets_feature_extractor):
        logging.info("SumMatches: starting")
        targets_feature_extractor.initialize()
        for cnt in self['counts']:
            logging.info('integrating pin')
            for k,v in cnt.iteritems():
                dec_target = targets_feature_extractor.decode_target(k[0])
                dec_feature = targets_feature_extractor.decode_feature(k[1])
                #FIXME: ad-hoc solution
                fk = "\t".join(["<-->".join(dec_target),dec_feature])
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
        sum_module.set_args(targets_features_extractor)
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

