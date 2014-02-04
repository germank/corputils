from clutils import *
from readers import DPCorpusReader
from counter import *
from utils import *
import logging

BUF_SIZE = 100000
SYNCRONIC = True
from collections import Counter
class StreamingCountMatches(JobModule):
    def __init__(self, work_path, counter, targets_features_extractor, 
                 corpus_file, gzip, target_format, context_format, sentence_separator):
        super(StreamingCountMatches, self).__init__(os.path.join(work_path))
        self.targets_features_extractor = targets_features_extractor
        self.target_format = target_format
        self.context_format = context_format
        self.sentence_separator = sentence_separator
        self.setup(TextFilePin(self, 'corpus', corpus_file, gzip),
            ProxyPin(self, 'output', counter))

    def run(self):
        logging.getLogger().setLevel(logging.INFO)#FIXME: move away
        logging.info("CountMatches({0}): starting "
        "counting".format(self.work_path))
        #FIXME: parametrize
        corpus_reader = DPCorpusReader(self['corpus'].read(), 
                                       separator=self.sentence_separator)
        for target, feature in self.targets_features_extractor(corpus_reader):
            self['output'].count(target.format(self.target_format),
                                    feature.format(self.context_format))
        logging.info("CountMatches: saving results")
        self['output'].join()
        while len(self['output'])>0:
            self['output'].save()
        logging.info("CountMatches({0}): finished "
        "counting".format(self.work_path))

    def finished(self):
        return False

#class CountMatches(JobModule):
#    def __init__(self, work_path, corpus_file, gzip):
#        super(CountMatches, self).__init__(work_path, 
#            name=os.path.basename(corpus_file))
#        self.setup(TextFilePin(self, 'corpus', corpus_file, gzip),
#            ValuePin(self, 'output'))
#
#    def run(self):
#        logging.getLogger().setLevel(logging.INFO)#FIXME: move away
#        logging.info("CountMatches: starting counting")
#        ret = Counter()
#        for sentence in DPCorpusReader(self['corpus'].read()):
#            for t in sentence:
#                ret[t[0].lower()] += 1
#        logging.info("CountMatches: saving results")
#        self['output'].write(ret)
#        logging.info("CountMatches: finished")
class CountMatches(JobModule):
    def __init__(self, work_path, targets_features_extractor, corpus_file, gzip, 
                 target_format, context_format, sentence_separator, to_lower):
        super(CountMatches, self).__init__(os.path.join(work_path, 
            os.path.basename(corpus_file)))
        self.targets_features_extractor = targets_features_extractor
        self.target_format = target_format
        self.context_format = context_format
        self.sentence_separator = sentence_separator
        self.to_lower = to_lower
        self.setup(TextFilePin(self, 'corpus', corpus_file, gzip),
                   ValuePin(self, 'output'))
        #PersistentCounterPin(self, 'output', 
        #    SqliteDestination(os.path.join(self.work_path, 'output.db'), 100)))

    def run(self):
        logging.getLogger().setLevel(logging.INFO)#FIXME: move away
        logging.info("CountMatches({0}): starting "
        "counting".format(self.work_path))
        #FIXME parametrixe
        sentence_filter = lambda t: t[2][0] in 'NJVR'
        
        corpus_reader = DPCorpusReader(self['corpus'].read(), 
                                       sentence_filter=sentence_filter,
                                       separator=self.sentence_separator,
                                       to_lower=self.to_lower)
        ret = Counter()
        for i,(target, feature) in \
            enumerate(self.targets_features_extractor(corpus_reader)):
            if (i+1) % 100000 == 0:
                logging.info("CountMatches({0}): {1} features extracted "
                             "so far...".format(self.work_path, i+1)) 
            ret[(target.format(self.target_format), 
                feature.format(self.context_format))] += 1
        logging.info("CountMatches: saving results")
        self['output'].write(ret)
        logging.info("CountMatches: finished")

    def finished(self):
        return self['output'].file_exists()
        


class SumResults(JobModule):
    def __init__(self, work_path):
        super(SumResults, self).__init__(work_path)
        self.setup(ListPin(self, 'counts'),
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

class KyotoServerManager(DieAfterDone):
    def __init__(self, host, port, db_name):
        super(KyotoServerManager, self).__init__(['ssh', '-t', host,
            'shopt -s huponexit; ktserver -port {0} -scr {1} {2}'.format(
            port,
            os.path.join(os.path.dirname(__file__), 'kyototycoon_ext.lua'), db_name)], True)
        self.host = host
        self.port = port

    def get_host(self):
        return self.host

    def get_port(self):
        return self.port
        
class CountSumPipeline(Pipeline):
    def __init__(self, work_path, targets_features_extractor, corpora, gzip,
    target_format, context_format, sentence_separator, to_lower):
        super(CountSumPipeline, self).__init__(work_path)
        sum_module = SumResults(os.path.join(work_path, 'sum_matches'))
        count_modules = []
        for corpus in corpora:
            count_module = CountMatches(os.path.join(work_path,
                'count_matches'), targets_features_extractor, corpus, gzip,
                target_format, context_format, sentence_separator, to_lower)
            
            sum_module['counts'].append(count_module['output'])
            #FIXME: change for equivalent
            #count_module['output'].connect_to(sum_module['counts'])
            count_modules.append(count_module)
        self.add_stage(*count_modules)
        self.add_stage(sum_module)

class DummyDestination:
    def save(self, counter):
        counter.cooccurrences.clear()
        pass
    def open(self):
        pass
    def close(self):
        pass

class DummyCounter:
    def count(self, target, feature):
        pass
    def open(self):
        pass
    def close(self):
        pass
    def __len__(self):
        return 0
    def join(self):
        pass
    
class StreamingCountPipeline(Pipeline):
    def __init__(self, db_host, db_port, work_path, targets_features_extractor, corpora, gzip,
    target_format, context_format, sentence_separator):
        super(StreamingCountPipeline, self).__init__(work_path)
        buf_size=BUF_SIZE
        syncronic=SYNCRONIC
        db = KyotoServerManager(db_host, db_port, os.path.join(work_path,
            'result.kct'))
        count_modules = []
        for corpus in corpora:
            counter = PersistentCounter(KyotoDestination(db), buf_size, syncronic) 
            #counter = PersistentCounter(DummyDestination(), buf_size, syncronic)#KyotoDestination(db), buf_size, syncronic) 
            count_module = StreamingCountMatches(os.path.join(work_path,
                'count_matches'), counter, targets_features_extractor, corpus, gzip,
                target_format, context_format)
            count_modules.append(count_module)
        self.add_context_mgr(db)
        self.add_stage(*count_modules)
