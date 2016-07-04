# -*- coding: utf-8 -*-
import json, sys, os, time, datetime, re, logging, string
from tokenizer import tokenize
from pymongo import MongoClient
from itertools import chain, islice
from collections import Counter

def _get_mongo_collection(mongo_server = 'mongodb://localhost:27017/', db_name = 'news_db', collection_name = 'docs'):
    client = MongoClient(mongo_server)
    db = client[db_name]
    return db[collection_name]

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
    returns a sorted list of term frequencies tuples [ ('term': frequency), .. ]
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

def main(argv):
    """
    """
    start_time = time.time()

    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

    input_dir = 'D:/Study/DCU/MCM/Datasets/nytimes/raw_data/sundayreview'
    ngram_size = 1
    filename_regex = '(?i).*(\.json)$'
    date_format = '%Y-%m-%d' # date_format = '%Y-%m-%d' if has_time else '%Y-%m-%d %H:%M:%S'
    source = 'nytimes'
    fixed_tags = ['review']
    mongo_server = 'mongodb://localhost:27017/'
    db_name = 'test_news_db2'
    collection_name = 'docs'
    chunk_size = 1000

    db = _get_mongo_collection(mongo_server, db_name, collection_name)
    # insert many chunk in chunk until the iterable is consumed
    logging.info('processing folder %s', input_dir)
    for chunk in _chunks(_process_all_files(input_dir, source, date_format, fixed_tags, ngram_size, filename_regex), chunk_size):
        res = db.insert_many(chunk)
        logging.info('inserted %d items into db', len(res.inserted_ids))

    logging.info('finished processing in %f seconds', time.time() - start_time)

if __name__ == "__main__":
    main(sys.argv[1:])
