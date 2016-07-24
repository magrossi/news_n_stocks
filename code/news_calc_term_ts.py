import logging, time, datetime, math, heapq, pandas as pd
from bson.code import Code

def get_tfidf_ts(input_collection, terms, filter):
    '''
    returns len(terms) time series with count, doc_count and tfidf for each term
    '''
    start_time = time.time()
    logging.info('finding term time series with %s', filter)
    cursor = input_collection.find(filter)
    ct = 0
    data = {}  # data for ts of term
    idx = {} # index for ts of term
    # build data and index arrays
    for term in terms:
        idx[term] = []
        data[term] = []
    for dsum in cursor:
        ct += 1
        for term in terms:
            if term in dsum['value']['term_counts']:
                data[term].append(dsum['value']['term_counts'][term]) # counts = [count, doc_count, tfidf]
                idx[term].append(dsum['value']['date']) # index date
    # build pandas dataframe out of previously calcualted arrays
    dfs = {}
    for term in terms:
        dfs[term] = pd.DataFrame(data[term], index=idx[term], columns=['Count', 'Document Count', 'TF-IDF Score'])
    return pd.Panel(dfs)
    logging.info('finished processing %d documents in %f seconds', ct, time.time() - start_time)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

    from pymongo import MongoClient
    mongo_server = 'mongodb://localhost:27017/'
    db_name = 'test_news_db2'
    collection_name = 'daily_summary'
    filter = {
        '_id': {
            '$gte': datetime.datetime(2000, 01, 01),
            '$lt': datetime.datetime(2100, 01, 02)
        }
    }

    client = MongoClient(mongo_server)
    db = client[db_name]
    coll = db[collection_name]

    terms = ['night', 'day']

    logging.debug('testing get_tfidf_ts function for terms %s', ', '.join(terms))
    panel = get_tfidf_ts(coll, terms, filter)
    logging.debug(panel)

    pass
