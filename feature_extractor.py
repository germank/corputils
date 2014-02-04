import logging
import re

def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

class TargetsFeaturesExtractor():
    '''
    main loop of the feature-extraction procedure
    '''
    def __init__(self, matchers, feature_extractor, target_format,
                 context_format, targets):
        self.matchers = matchers
        self.feature_extrator = feature_extractor
        self.target_format = target_format
        self.context_format = context_format
        self.targets = targets

    def __call__(self, corpus_reader):
        matchers = self.matchers
        feature_extractor = self.feature_extrator
        #FIXME: put in some better place
        if self.targets:
            try:
                #be ambitious!
                grp_targets = ["|".join(chnk) for chnk in chunks(list(self.targets),1000)]
            except:
                #surrender to mediocrity!
                grp_targets = ["|".join(chnk) for chnk in chunks(list(self.targets),100)]
            targets_re = [re.compile(w) for w in grp_targets]
            targets_filter = lambda w,targets_re=targets_re: any(m.match(w) 
                            for m in targets_re)
        else:
            targets_filter = None
            
        #a chunk is usually a sentence (we cannot get features passed the chunk)
        for chunk in corpus_reader:
            try:
                seen_pairs = set()
                for matcher in matchers:
                    for target in matcher.get_matches(chunk):
                        #skip targets that are not in the specified list
                        if targets_filter and \
                            not targets_filter(target.format(self.target_format)):
                            continue
                        for feature in feature_extractor.get_features(target, chunk):
                            if (target, feature) not in seen_pairs:
                                seen_pairs.add((target,feature))
                                yield target, feature
            except IOError:
                raise
            except StandardError:
                logging.exception("Error while processing sentence: {0}".format(
                    chunk))

class LexicalFeature(object):
    def __init__(self, chunk, pm, token):
        '''
        pm: a positional marker such as "l" or "r"
        token:  the token used as a feature
        '''
        self.chunk = chunk
        self.pm = pm
        self.token = token

    def format(self, fmt):
        #Make format customizable
        return "{0}".format(self.token.format(fmt))
        #return "{0}\t{1}".format(self.pm, self.token.format(fmt))

    def __eq__(self, ot):
        '''
        Equality only depends on the token, not on it's relative position
        '''
        return self.chunk.get_token_pos(self.token) ==\
            self.chunk.get_token_pos(ot.token)

    def __hash__(self):
        return self.chunk.get_token_pos(self.token)

    def __repr__(self):
        return "LexicalFeature({0}, {1})".format(self.pm, self.token)

class BOWFeatureExtractor(object):
    '''
    Extracts lexical co-occurrence features
    '''
    def __init__(self, w, context_words, context_format):
        '''
        w: window (takes w tokens to each side)
        '''
        self.w = w 
        self.context_words = context_words
        self.context_format = context_format
    
    def is_valid_feature(self, t):
        return not self.context_words or t.format(self.context_format) in\
            self.context_words

    def get_features(self, target, chunk):
        #FIXME: generalize!!!!!
        if len(target.tokens) == 1:
            i = chunk.get_token_pos(target.tokens[0])
            #only care about linear order
            sentence = chunk.linear()
            w = self.w
            #print coocurrences
            lend = max(0,i-w) if w else 0
            for lt in sentence[lend:i]:
                if self.is_valid_feature(lt):
                    yield LexicalFeature(chunk, "l", lt)
            rend = min(len(sentence),i+(w+1)) if w else len(sentence)
            for rt in sentence[i+1:rend]:
                if self.is_valid_feature(rt):
                    yield LexicalFeature(chunk, "r", rt)
        else:
            #for the time being, asume bigram match
            #FIXME: generalize to n-gram
            t, comp_t = target.tokens
            t_pos = chunk.get_token_pos(t)
            comp_t_pos = chunk.get_token_pos(comp_t)
            #only care about linear order
            sentence = chunk.linear()
            #put the composed words in order
            if t_pos < comp_t_pos:
                lcomp_t = t
                rcomp_t = comp_t
                l = t_pos
                r = comp_t_pos
            else:
                lcomp_t = comp_t
                rcomp_t = t
                l = comp_t_pos
                r = t_pos
            #yield coocurrences
            w = self.w
            #count left of first composed word
            #and print context as "l" (left)
            lend = max(0,l-w) if w else 0
            for lt in sentence[lend:l]:
                if self.is_valid_feature(lt):
                    yield LexicalFeature(chunk, "l", lt)
            #count right of first and left of second in range of
            #the first
            #and print context as "c" (center)
            #FIXME: is this even useful?
            lmid = min(r,l+(w+1)) if w else r
            for ct in sentence[l+1:lmid]:
                if self.is_valid_feature(ct):
                    yield LexicalFeature(chunk, "c", ct)
            #count right of first and left of second in range of
            #the second
            #and print context as "r" (right)
            rmid = max(lmid,r-w) if w else r
            for ct in sentence[rmid:r]:
                if self.is_valid_feature(ct):
                    yield LexicalFeature(chunk, "c", ct)
            rend = min(len(sentence), r+(w+1)) if w else \
                len(sentence)
            for rt in sentence[r+1:rend]:
                if self.is_valid_feature(rt):
                    yield LexicalFeature(chunk, "r", rt)
