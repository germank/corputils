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

class PeripheralLinearBigramMatcher():
    def __init__(self, linear_comp):
        '''linear_comp: A pseudo-regular expression to match 
        composition phrases bases on linear order.
        Each token is represented with a T<> marker which can 
        take as optional arguments "word" and "pos". 
        E.g. T<word=big,pos=JJ>(T<pos=JJ>)*T<word=file(rows.txt),pos=NN|NNS>'''
        
        if not linear_comp:
            self.linear_comp_match = None
        def token_expr(expr):
            '''Auxiliary function that returns a regular expression
            that matches tokens in the corpus the specification
            inside a T<> expression'''
            ret_exprs = list(repeat(r"[^\t\|]*?", 6))
            
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
                return value
            
            for _, kw, value in re.findall(r'(([^,=]+)=([^,=]+))', expr):
                if kw == 'pos':
                    ret_exprs[2] = "({0})".format(_aux_value_to_regexp(value))
                if kw == 'word':
                    ret_exprs[1] = "({0})".format(_aux_value_to_regexp(value))
            return '\\t'.join(ret_exprs)
        expr = re.sub('T<(.*?)>', lambda m:r"\|({0})".format(token_expr(m.group(1))), linear_comp) + r"\|"
        self.linear_comp_match = re.compile(expr)
    
    def get_matches(self, sentence, plain_text_sentence):
        ret = []
        if self.linear_comp_match:
            #we transform the sentence into a string
            sentence_str = "|{0}|".format("|".join(plain_text_sentence))
            token_lens = [len(t)+1 for t in plain_text_sentence]
            partial_sums = [sum(token_lens[:i]) for i in range(len(token_lens)+1)]
            #token_lims = [(s,e-1) for s,e in zip(partial_sums[:-1], partial_sums[1:])] 
            token_start2pos = {s:i for i,s in enumerate(partial_sums[:-1])}
            token_end2pos = {e:i for i,e in enumerate(partial_sums[1:])}
            #for each match of the pseudo-regexp in the sentence
            for i,m in enumerate(self.linear_comp_match.finditer(sentence_str)):
                #obtain the matched tokens
                left_match_pos = token_start2pos[m.start(0)]
                right_match_pos = token_end2pos[m.end(0)-1]
                ret.append((sentence[left_match_pos], 
                            sentence[right_match_pos]))
        return ret

class PeripheralDependencyBigramMatcher():
    def __init__(self, lword, lpos, lfile, rword, rpos, rfile):
        '''Matches bigrams across dependency arcs'''
        self.left_comp_match = self._build_composition_match_func(lword, lpos, lfile)
        self.right_comp_match = self._build_composition_match_func(rword, rpos, rfile)
    
    def composition_target(self,  t, sentence):
        '''If t is a matching left node, then return
        the node which is dependent upon'''
        if self.left_comp_match(t) and t[4] > 0:
            comp_t = sentence[t[4]-1]
            assert comp_t[3] == t[4]
            if self.right_comp_match(comp_t):
                return comp_t
        return None

    def get_matches(self, sentence, _):
        comp_matches = []
        for i, t in enumerate(sentence): #i,t = index,tuple in sentence
            comp_t = self.composition_target(t, sentence)
            if comp_t:
                comp_matches.append((t,comp_t))
        return comp_matches

    def _build_composition_match_func(self, word_regexp, pos_regexp, wordset_file):
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
                match_func = partial(lambda f, w: f(w) and word_regexp_func(w[1]),
                    match_func)
            else:
                match_func = lambda w: word_regexp_func(w[1])
        if pos_regexp:
            pos_regexp_func = re.compile(pos_regexp, re.I).match
            if match_func:
                match_func = partial(lambda f, w: f(w) and pos_regexp_func(w[2]),
                    match_func)
            else:
                match_func = lambda w: pos_regexp_func(w[2])
        if wordset_file:
            wordset = load_words(wordset_file)
            in_wordset = wordset.__contains__
            if match_func:
                #FIXME: by using the index -2 we are referring to the formatted
                #element. Bug or feature?
                match_func = partial(lambda f, w: f(w) and in_wordset(w[-2]),
                    match_func)
            else:
                match_func = lambda w: in_wordset(w[-2])
    
        if match_func:
            return match_func
        else:
            return lambda w: True

