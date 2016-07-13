import logging, time, datetime, math, heapq
from bson.code import Code

def get_tfidf_ts(input_collection, terms, filter):
    '''
    returns len(terms) time series with count, doc_count and tfidf for each term
    '''
    start_time = time.time()
    logging.info('finding term time series with %s', filter)
    cursor = input_collection.find(filter)
    ct = 0
    terms = {}
    for dsum in cursor:
        pass # to do
    logging.info('finished processing in %f seconds', time.time() - start_time)

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
    get_tfidf_ts(coll, ['term1', 'term2'], filter)
    pass
