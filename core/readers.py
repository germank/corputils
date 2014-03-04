import logging
import weakref

class Token(object):
    '''
    Represents a token in the corpus.
    Importantly, the token is assumed to be uniquely identified by the "data".
    In the DP corpora this is true as long as the tokens belong to one
    sentence. However, when the sentence boundary is crossed, this assumption
    is usually violated. For that reason, it might be required to add an ad-hoc
    attribute to uniquely identify each sentence.
    '''
    def __init__(self, chunk, data):
        self.chunk = chunk
        self.data = data
        #FIXME: ad-hoc; move
        self.data['cat']=data['pos'][0].lower()

    def __getitem__(self, k):
        return self.data[k]

    def __setitem__(self, k, v):
        self.data[k] = v

    def format(self, fmt):
        return fmt.format(**self.data)

    def __str__(self):
        return str(self.data)

    def __eq__(self, ot):
        return self.chunk.get_token_pos(self) == ot.chunk.get_token_pos(ot)
        #return self.data == ot.data

    def __hash__(self):
        return self.chunk.get_token_pos(self)
        #try:
        #    return self._hash
        #except AttributeError:
        #    #this might be a relly inefficient computation, so we better store it
        #    t = tuple((k, self.data[k]) for k in sorted(self.data.keys()))
        #    self._hash = hash(t)
        #    return self._hash

    def __repr__(self):
        return "Token({0})".format(self.data)

from collections import OrderedDict
class Sentence(object):
    def __init__(self, corp_format, corp_types, to_lower) :
        self.tokens = OrderedDict()
        self.linear_tokens = []
        #self.rev_linear_tokens = {}
        self.plain_text = []
        self.corp_format = corp_format
        self.corp_types = corp_types
        self.to_lower = to_lower

    def push_token(self, line, splitted_line=None):
        self.plain_text.append(line)
        if not splitted_line:
            t = line.split('\t')
        else:
            t = splitted_line
        data = dict(zip(self.corp_format, t))
        if self.to_lower:
            data['word'] = data['word'].lower()
            data['lemma'] = data['lemma'].lower()
        for k,conv in self.corp_types:
            data[k] = conv(data[k])
        token = Token(weakref.proxy(self), data)
        self.linear_tokens.append(token)
        self.tokens[data['id']] = token
        token['sentence_pos'] = len(self.linear_tokens)-1
        #self.rev_linear_tokens[token] = len(self.linear_tokens)-1

    def get_token_pos(self, token):
        return token['sentence_pos']
        #return self.rev_linear_tokens[token]

    def linear(self):
        return self.linear_tokens

    def __getitem__(self, k):
        return self.tokens[k]

    def __iter__(self):
        return iter(self.tokens.values())

    def get_plain_text(self):
        return self.plain_text

    def __str__(self):
        return "\n".join(self.plain_text)

class DPCorpusReader(object):
    '''
    Reads sentences from dependency parsed corpora
    '''
    def __init__(self, corpora, separator='s', to_lower=False):
        self.end_separator = '/{0}'.format(separator)
        self.corpora = corpora
        self.corp_format = ('word', 'lemma', 'pos', 'id', 'dep_id',
            'dep_rel')
        self.corp_types = {}#{'id': int, 'dep_id': int} #is it needed?
        self.to_lower = to_lower

    def __iter__(self):
        return self

    def next(self):
        sentence = Sentence(self.corp_format, self.corp_types, self.to_lower)
        for line in self.corpora:
            line = line.rstrip('\n')
            if line.strip('<>') == self.end_separator:
                return sentence
            elif line[0] == '<' and line[-1] == '>':
                #skip beggining of sentence or text markers
                continue 
            else:
                splitted_line = line.split("\t")
                sentence.push_token(line, splitted_line)
        raise StopIteration
