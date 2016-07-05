import logging, time, datetime
from bson.code import Code

def _mapper():
    return Code("""
            function () {
                var date = this.date,
                    count = 0;
                this.bag_of_words.forEach(function (w) {
                    w.push(this._id) // add doc id for counting doc number later
                    emit(date, w);   // w = [term, freq, doc_id]
                });
            }
            """)

def _reducer():
    return Code("""
            function (dt, term_freqs) {
                var d = { date: dt, doc_count: 0, term_freq: [] },
                    unique_docs = {},
                    dic_terms = {};
                // calculate the daily term frequencies and number of docs
                term_freqs.forEach(function (tf) {
                    // term frequencies
                    if (dic_terms[tf[0]]) {
                        dic_terms[tf[0]] += tf[1];
                    } else {
                        dic_terms[tf[0]] = tf[1];
                    }
                    // count distinct documents
                    if (!unique_docs[tf[2]]) {
                        d.doc_count++;
                    } else {
                        unique_docs[tf[2]] = 1 // so its not "falsy"
                    }
                });
                // trim down resulting term list to 5000 (otherwise too much useless data)
                // copy dictionary to array
                var array_terms = [];
                for (var term in dic_terms) array_terms.push([term, dic_terms[term]]);
                // sort array desc
                array_terms.sort(function(a, b) { return b[1] - a[1]; });
                // take first 5000
                d.term_freq = array_terms.slice(0, 5000);
                return d;
            }
            """)

def _get_mongo_collection(mongo_server = 'mongodb://localhost:27017/', db_name = 'news_db', collection_name = 'docs'):
    client = MongoClient(mongo_server)
    db = client[db_name]
    return db[collection_name]

def calculate_daily_summary(mongo_collection, result_collection_name, filter={}):
    start_time = time.time()
    logging.info('applying map reduce with filter %s', filter)
    result = mongo_collection.map_reduce(_mapper(), _reducer(), result_collection_name, query=filter, full_response=True)
    logging.info(result)
    logging.info('finished processing in %f seconds', time.time() - start_time)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

    from pymongo import MongoClient
    mongo_server = 'mongodb://localhost:27017/'
    db_name = 'test_news_db2'
    collection_name = 'docs'
    mr_collection_name = 'daily_summary'
    filter = {
        'date': {
            '$gte': datetime.datetime(2000, 01, 01),
            '$lt': datetime.datetime(2100, 01, 02)
        }
    }

    client = MongoClient(mongo_server)
    db = client[db_name]
    coll = db[collection_name]
    calculate_daily_summary(coll, mr_collection_name, filter)
