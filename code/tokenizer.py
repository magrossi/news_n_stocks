# -*- coding: utf-8 -*-
from nltk import pos_tag
from nltk.util import ngrams
from nltk.corpus import wordnet
from nltk.tokenize import word_tokenize
from replacers import RegexpReplacer

def _get_wordnet_pos(treebank_tag):
    if treebank_tag.startswith('J'):
        return wordnet.ADJ
    elif treebank_tag.startswith('V'):
        return wordnet.VERB
    elif treebank_tag.startswith('N'):
        return wordnet.NOUN
    elif treebank_tag.startswith('R'):
        return wordnet.ADV
    else:
        return wordnet.NOUN # this is the default

def _isValid(word):
    # clear invalid words
    # words with numbers in them?
    # is it worth removing it?
    return True

def tokenize(raw, sizes = [1]):
    ''' Tokenizes raw text into word n-grams of sizes (n in sizes).
    Applies normal pre-processing techniques:
    - Removes stopwords and words with length smaller than three
    - Expands contractions
    - Splits raw text by sentence and converts text to lowercase
    - Lemmatizes words based on position (better than stemming)
    >>> list(tokenize(u'My super text. It's awesome!', [1,2,3]))
    [u'super', u'text', u'super text', u'awesome']
    '''
    # load expensive imports only once even if you call the function multiple times
    # http://blender.stackexchange.com/questions/2665/how-can-i-do-a-one-time-initialization
    # answer by: ideasman42
    if tokenize.pre_load is None:
        # logs initialization
        import logging
        logging.debug('initializing tokenize...')
        tokenize.pre_load = {}
        # stopwords dictionary
        from nltk.corpus import stopwords
        tokenize.pre_load['stopwords'] = set(stopwords.words('english'))
        # PunktSentenceTokenizer for english
        import nltk.data
        tokenize.pre_load['sent_tokenizer'] = nltk.data.load('tokenizers/punkt/english.pickle')
        # PorterStemmer stemmer
        from nltk.stem import PorterStemmer
        tokenize.pre_load['stemmer'] = PorterStemmer()
        # WordNetLemmatizer lemmatizer
        from nltk.stem import WordNetLemmatizer
        tokenize.pre_load['lemmatizer'] = WordNetLemmatizer()
        # lemmatizer.lemmatize('cooking', pos='v')
        # use nltk.pos_tag() then convert using get_wordnet_pos()
        # must analyze the structure of the sentence first
        logging.debug('tokenize initialized')

    # returns word ngrams up to max_size
    for sentence in tokenize.pre_load['sent_tokenizer'].tokenize(raw):
        # tokenize sentence into words removing stopwords and punctuation
        raw_words = [w for w in word_tokenize(RegexpReplacer().replace(sentence.lower())) if len(w) > 3 and w not in tokenize.pre_load['stopwords'] and _isValid(w)]
        # tag words and lemmatize (instead of stemming)
        words = [tokenize.pre_load['lemmatizer'].lemmatize(tw[0], pos=_get_wordnet_pos(tw[1])) for tw in pos_tag(raw_words)]
        # then return ngrams of sizes of the resulting bag of words
        for i in sizes:
            for w in ngrams(words, i):
                yield ' '.join(w)

tokenize.pre_load = None

if __name__ == "__main__":
    print list(tokenize(u'My super text. It\'s awesome! Be careful of terrorist attacks!', [2, 3]))
