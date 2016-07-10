import logging, time, datetime, math, heapq
from bson.code import Code

def calculate_period_summary(input_collection, output_collection, period_type, max_terms=1000, filter={}):
    '''
    Merges daily summary type documents into a merged document of the format:
    {
        _id: <auto>,
        period_type: 'daily', // daily, weekly, monthly, bi-yearly, yearly,.. custom..
        period_start: <min date>,
        period_end: <max date>,
        total_docs: <total docs in period>,
        total_terms: <total terms in period>,
        term_counts: [        // sorted by tfidf score and limited to 'n' top entries
            [term, frequency, doc frequency, tfidf score],
            ...
        ]
    }
    Saves the results into result_collection_name.
    '''
    start_time = time.time()
    logging.info('finding daily summaries with filter %s ', filter)
    cursor = input_collection.find(filter)
    ct = 0
    summ = {
        'period_type': period_type,
        'period_start': datetime.datetime.max,
        'period_end': datetime.datetime.min,
        'total_docs': 0,
        'total_terms': 0,
        'term_counts': []
    }
    terms = {}
    for dsum in cursor:
        ct += 1
        summ['period_start'] = summ['period_start'] if summ['period_start'] < dsum['value']['date'] else dsum['value']['date']
        summ['period_end'] = summ['period_end'] if summ['period_end'] > dsum['value']['date'] else dsum['value']['date']
        summ['total_docs'] += dsum['value']['total_docs']
        summ['total_terms'] += dsum['value']['total_terms']
        for term, counts in dsum['value']['term_counts'].iteritems():
            if term in terms:
                terms[term][0] += counts[0] # ter frequency
                terms[term][1] += counts[1] # doc frequency
            else:
                terms[term] = counts # tfidf score will be overritten later
    terms_arr = []
    for term, counts in terms.iteritems():
        tf = counts[0] / summ['total_terms']
        idf = math.log(summ['total_docs'] / counts[1])
        terms_arr.append([term, counts[0], counts[1], tf * idf])
    summ['term_counts'] = heapq.nlargest(max_terms, terms_arr, key=lambda x: x[3])
    logging.info('total of %d documents processed', ct)
    logging.info('saving to out collection...')
    output_collection.insert(summ)
    logging.info('finished processing in %f seconds', time.time() - start_time)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

    from pymongo import MongoClient
    mongo_server = 'mongodb://localhost:27017/'
    db_name = 'test_news_db2'
    collection_name = 'daily_summary'
    out_collection_name = 'period_summary'
    filter = {
        '_id': {
            '$gte': datetime.datetime(2000, 01, 01),
            '$lt': datetime.datetime(2100, 01, 02)
        }
    }

    client = MongoClient(mongo_server)
    db = client[db_name]
    coll = db[collection_name]
    out_coll = db[out_collection_name]
    calculate_period_summary(coll, out_coll, 'all', 10, filter)
    pass
