# -*- coding: utf-8 -*-
import json, sys, os, time, datetime, re, logging, string
from tokenizer import tokenize
from itertools import chain, islice
from collections import Counter

def _process_file(filename, source, date_format, fixed_tags=[], ngram_size=1):
    try:
        with open(filename, 'rb') as f:
            # Load article in JSON format
            # {
            #     "author":"Adriana Gardella",
            #     "category":"BusinessDay",
            #     "date":"2013-01-11",
            #     "description":"I always want to...",
            #     "text":"In my last , She Owns It  members..",
            #     "textType":"Blogs",
            #     "title":"Business Owners Reflect...",
            #     "url":"http://boss.blogs.nytimes.com/2013/01/11/business-owners-reflect-on-small-victories-and-the-dangers-of-growth/"
            # }
            data = json.load(f)
            # {
            #     source: 'nytimes',
            #     date: '2013-01-01',
            #     tags: [],
            #     title: 'title',
            #     url: 'http://dealbook.nytimes.com/2013/01/01/ackman-herbalife-and-celebrity-short-sellers/',
            #     bag_of_words: []
            # }
            return {
                'source': source,
                'date': datetime.datetime.strptime(data['date'], date_format),
                'tags': fixed_tags,
                'title': data['title'],
                'url': data['url'],
                'bag_of_words': _get_term_frequencies(tokenize(data['text'], ngram_size))
            }
    except Exception, e:
        logging.error('processing %s with error %s', filename, e.message)
        return None

def _get_term_frequencies(tokens):
    '''
    returns a sorted list of sorted term frequencies tuples [ ('term': frequency), .. ]
    '''
    return list(Counter(tokens).most_common())

def _get_files_to_process(initial_dir, match_regex=None, recursive=True):
    for root, dirs, files in os.walk(initial_dir):
        for f in files:
            if match_regex is None or re.search(match_regex, f):
                yield os.path.join(root, f)

def _process_all_files(initial_dir, source, date_format, fixed_tags=[], ngram_size=1, match_regex=None, recursive=True):
    processed = 0
    errored = 0
    for filename in _get_files_to_process(initial_dir, match_regex, recursive):
        doc = _process_file(filename, source, date_format, fixed_tags, ngram_size)
        if doc is not None and len(doc['bag_of_words']) > 0:
            yield doc
            processed += 1
        else:
            errored += 1
    logging.info('%d docs processed %d successfull and %d errored', processed + errored, processed, errored)

def _chunks(iterable, size=100):
    iterator = iter(iterable)
    for first in iterator:
        yield list(chain([first], islice(iterator, size - 1)))

class InputItem(object):
    '''
    helper class to hold input configuration
    '''
    def __init__(self, dir, source='', tags=[], date_format='%Y-%m-%d', file_match_regex='(?i).*(\.json)$'):
        self.dir = dir
        self.source = source
        self.tags = tags
        self.date_format = date_format
        self.regex = file_match_regex

    def get_conf(self):
        return (self.dir, self.source, self.tags, self.date_format, self.regex)

def process_items(inputs, mongo_collection, ngram_size=1, insert_after=1000):
    logging.info('starting to process %d items', len(inputs))
    start_total_time = time.time()
    total_inserted = 0
    for dir, source, tags, date_format, regex in map((lambda i: i.get_conf()), inputs):
        partial_inserted = 0
        try:
            start_time = time.time()
            logging.info('processing item [dir: %s, source: %s, tags: %s, date_format: %s, regex: %s', dir, source, tags, date_format, regex)
            for chunk in _chunks(_process_all_files(dir, source, date_format, tags, ngram_size, regex), insert_after):
                res = mongo_collection.insert_many(chunk)
                logging.info('inserted %d items into db', len(res.inserted_ids))
                partial_inserted += len(res.inserted_ids)
            logging.info('finished processing %d items on dir %s in %f seconds, avg of %f seconds per item', total_inserted, dir, time.time() - start_time, partial_inserted if partial_inserted <= 0 else (time.time() - start_time)/partial_inserted)
        except Exception, e:
            logging.error('failed processing dir %s with error %s after %f seconds', dir, e.message, time.time() - start_time)
        total_inserted += partial_inserted
    logging.info('finished processing %d items for all dirs in %f seconds', total_inserted, time.time() - start_total_time)

if __name__ == "__main__":
    # configure logger
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')
    # test config params
    input_dir = 'D:/Study/DCU/MCM/Datasets/nytimes/raw_data/sundayreview'
    ngram_size = 1
    filename_regex = '(?i).*(\.json)$'
    date_format = '%Y-%m-%d' # date_format = '%Y-%m-%d' if has_time else '%Y-%m-%d %H:%M:%S'
    source = 'nytimes'
    fixed_tags = ['review']
    mongo_server = 'mongodb://localhost:27017/'
    db_name = 'test_news_db2'
    collection_name = 'docs'

    # get mongo collection to insert results to
    from pymongo import MongoClient
    client = MongoClient(mongo_server)
    db = client[db_name]
    coll = db[collection_name]

    # process all
    process_items([InputItem(dir=input_dir, source=source, tags=fixed_tags, date_format=date_format, file_match_regex=filename_regex)], coll, ngram_size=3, insert_after=1000)
