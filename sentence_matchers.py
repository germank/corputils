'''
A sentence matcher returns a list of N-gram (now, bigrams) respresented as tuples 
that match some particular criteria.
A matcher is an object containing the method get_matches which takes:
sentence: a list of sentences where each token is represented as a tuple containing
(word, lemma, pos, id, dep_id, dep_name, formatted identifier to output when it's a pivot,
formatted identifier to output when it's context)
and plain_text_sentence: a list of lines as read from a parsed corpus file. 
'''
import fileinput
import re
from functools import partial
from itertools import repeat

def load_words(filename):
    pivots = set()
    for line in fileinput.input(filename):
        pivots.add(line.strip(' \t\n'))
    return pivots

def get_composition_matchers(args):
    '''
    Represent a list of matchers based on the specification described in args.
    The specification is currently strongly coupled with the command arguments
    that both dpgrep and print_directional_bigrams take (ugly, but useful)
    '''
    match_funcs = []
    if args.linear_comp:
        match_funcs.append(PeripheralLinearBigramMatcher(args.linear_comp, ignore_case=args.ignore_case, token_sep=args.token_sep))

    if args.deprel or args.depword or args.deppos or args.depfile or args.headword or args.headpos or args.headfile or args.deplemma or args.headlemma:
        match_funcs.append(PeripheralDependencyBigramMatcher(args.deprel, args.depword, args.deplemma, args.deppos, args.depfile, 
        args.headword, args.headlemma, args.headpos, args.headfile,
        args.target_format, token_sep=args.token_sep))
    print match_funcs
    return match_funcs

class Match(object):
    '''
    represents a subset of a sentence.
    '''
    def __init__(self, tokens, tokens_annot = None, token_sep ='<-->'):
        '''
            tokens: the set of tokens we are representing
            tokens_annot: a dictionary of token->string to add some extra
            information (e.g. "target")
        '''
        self.tokens = tokens
        self.tokens_annot = tokens_annot if tokens_annot else {}
        self.token_sep = token_sep

    def has_marker(self, token, marker):
        return token in self.tokens_annot and marker ==\
            self.token_markers[token]

    def format(self, fmt):
        return self.token_sep.join(t.format(fmt) for t in self.tokens)

    def __repr__(self):
            return "Match({0})".format(self.tokens)

    def __hash__(self):
        try:
            return self._hash
        except:
            self._hash = hash(self.tokens)
            return self._hash

    def __eq__(self, ot):
        return self.tokens == ot.tokens


class UnigramMatcher():
    '''Matches single tokens for the core space'''

    def __init__(self, allowed_fwords=None, fwords_fmt=None):
        self.allowed_fwords = allowed_fwords
        self.fwords_fmt = fwords_fmt

    def get_matches(self, sentence):
        for token in sentence:
            if self.allowed_fwords and token.format(self.fwords_fmt) not in \
                self.allowed_fwords:
                continue
            yield Match((token,))
            

class PeripheralLinearBigramMatcher():
    '''Match phrases based on a pseudo-regular expression.
    Each token is represented with a T<> marker which can 
    take as optional arguments "word" and "pos". 
    E.g. T<word=big,pos=JJ>(T<pos=JJ>)*T<word=file(rows.txt),pos=NN|NNS>'''

    def __init__(self, linear_comp, ignore_case=False, token_sep='<-->'):
        if not linear_comp:
            self.linear_comp_match = None
        self.token_sep = token_sep
        def token_expr(expr):
            '''Auxiliary function that returns a regular expression
            that matches tokens in the corpus the specification
            inside a T<> expression'''
            ret_exprs = list(repeat(r"[^\t\|]*", 6))
            
            def _sanitize(value):
                return re.sub(r'(?<!\\)\.',  r'[^\\t\|]', value)
            
            def _aux_value_to_regexp(value):
                '''Auxiliary function that returns a regular expression
                for a value expression.
                value -> literal_string
                value -> file(filename) 
                ''' 
                file_value_expr = re.match("file\((.*?)\)", value)
                if file_value_expr:
                    value = file_value_expr.group(1) 
                    return "|".join(load_words(value))
                return _sanitize(value)
            
            for _, kw, value in re.findall(r'(([^,=]+)=([^,=]+))', expr):
                if kw == 'pos':
                    ret_exprs[2] = "({0})".format(_aux_value_to_regexp(value))
                if kw == 'word':
                    ret_exprs[0] = "({0})".format(_aux_value_to_regexp(value))
                if kw == 'lemma':
                    ret_exprs[1] = "({0})".format(_aux_value_to_regexp(value))
            return '\\t'.join(ret_exprs)
        #tokens are delimited by |
        #print list(filter(bool,re.split(r'(T<.*?>)', linear_comp)))
        #re.sub(r'T<(.*)>'
        #linear_comp = r"\|".join(filter(bool,re.split(r'(T<.*?>)', linear_comp)))
        expr = re.sub(r'T<(.*?)>', lambda m:r"(\|{0}\|)".format(token_expr(m.group(1))), linear_comp)
        self.expr = expr
        self.linear_comp_match = re.compile(expr, flags=re.IGNORECASE if
        ignore_case else 0 )
    
    def get_matches(self, sentence):
        plain_text_sentence = sentence.plain_text
        if self.linear_comp_match:
            #we transform the sentence into a string
            sep = "||"
            sentence_str = "|{0}|".format(sep.join(plain_text_sentence))
            token_lens = [len(t)+len(sep) for t in plain_text_sentence]
            partial_sums = [sum(token_lens[:i]) for i in range(len(token_lens)+1)]
            #token_lims = [(s,e-1) for s,e in zip(partial_sums[:-1], partial_sums[1:])] 
            token_start2pos = {s:i for i,s in enumerate(partial_sums[:-1])}
            token_end2pos = {e:i for i,e in enumerate(partial_sums[1:])}
            #for each match of the pseudo-regexp in the sentence
            for i,m in enumerate(self.linear_comp_match.finditer(sentence_str)):
                #obtain the matched tokens
                left_match_pos = token_start2pos[m.start(0)]
                right_match_pos = token_end2pos[m.end(0)]
                yield Match(tuple(sentence.linear()[left_match_pos:right_match_pos+1]), 
                            token_sep=self.token_sep)

class PeripheralDependencyBigramMatcher():
    def __init__(self, deprel, depword, deplemma, deppos, depfile, headword, headlemma, 
    headpos, headfile, filefmt, token_sep='<-->'):
        '''Matches bigrams across dependency arcs'''
        self.reprel = deprel
        self.token_sep = token_sep
        self.deprel = deprel
        self.depword = depword
        self.deplemma = deplemma
        self.deppos = deppos
        self.depfile = depfile
        self.headword = headword
        self.headlemma = headlemma 
        self.headpos = headpos
        self.headfile = headfile
        self.filefmt = filefmt
        self.dep_comp_match = None
        self.head_comp_match = None

    def lazy_init(self):
        if not self.dep_comp_match:
            self.dep_comp_match = \
            self._build_composition_match_func(self.depword, self.deplemma, 
            self.deppos, self.depfile, self.filefmt)
            self.head_comp_match =\
            self._build_composition_match_func(self.headword, self.headlemma,
            self.headpos, self.headfile, self.filefmt)
        
    
    def composition_target(self,  dep_t, sentence):
        '''If dep_t is a matching left node, then return
        the node which is dependent upon'''
        if self.dep_comp_match(dep_t) and dep_t['dep_id'] != '0':
            if self.reprel and not re.match(self.reprel, dep_t['dep_rel']):
                return None
            head_t = sentence[dep_t['dep_id']]
            assert head_t['id'] == dep_t['dep_id'], (dep_t, head_t)
            if self.head_comp_match(head_t):
                return head_t

        return None

    def get_matches(self, sentence):
        self.lazy_init()
        for i, dep_t in enumerate(sentence): #i,t = index,tuple in sentence
            head_t = self.composition_target(dep_t, sentence)
            if head_t:
                yield Match((dep_t,head_t), token_sep=self.token_sep)

    def _build_composition_match_func(self, word_regexp, lemma_regexp, pos_regexp, wordset_file, wordset_fmt):
        '''
        Returns a function that selects tokens in a DP.
        The criteria for the selection can be determined according to a regexp,
        a POS tag, or a file containing a set of lemmas
        '''
        #I've tried to build the function using the least possible amount of lambdas
        #Still, there are quite a lot
        match_func = None
        if word_regexp:
            word_regexp_func = re.compile(word_regexp, re.I).match
            if match_func:
                match_func = partial(lambda f, w: f(w) and word_regexp_func(w['word']),
                    match_func)
            else:
                match_func = lambda w: word_regexp_func(w['word'])
        if lemma_regexp:
            lemma_regexp_func = re.compile(lemma_regexp, re.I).match
            if match_func:
                match_func = partial(lambda f, w: f(w) and lemma_regexp_func(w['lemma']),
                    match_func)
            else:
                match_func = lambda w: lemma_regexp_func(w['lemma'])
        if pos_regexp:
            pos_regexp_func = re.compile(pos_regexp, re.I).match
            if match_func:
                match_func = partial(lambda f, w: f(w) and pos_regexp_func(w['pos']),
                    match_func)
            else:
                match_func = lambda w: pos_regexp_func(w['pos'])
        if wordset_file:
            wordset = load_words(wordset_file)
            in_wordset = wordset.__contains__
            if match_func:
                match_func = partial(lambda f, w: f(w) and
                    in_wordset(w.format(wordset_fmt)),
                    match_func)
            else:
                match_func = lambda w: in_wordset(w.format(wordset_fmt))
    
        if match_func:
            return match_func
        else:
            return lambda w: True

