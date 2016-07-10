import logging, time, datetime
from bson.code import Code

def _mapper():
    return Code("""
            function () {
                var date = this.date,
                    count = 0,
                    id = this._id;
                this.bag_of_words.forEach(function (w) {
                    w.push(id);    // add doc id for counting doc number later
                    emit(date, w); // w = [term, freq, doc_id]
                });
            }
            """)

def _reducer():
    return Code("""
            function (dt, tfs) {
                var d = { date: dt, total_docs: 0, total_terms: 0, term_counts: {} },
                    unique_docs = {};
                // calculate the daily term frequencies and number of docs
                tfs.forEach(function (tf) {
                    var term = tf[0],
                        freq = tf[1],
                        doc_id = tf[2];
                    // term frequencies (in corpus and docs it appears in)
                    if (d.term_counts[term]) {
                        d.term_counts[term][0] += freq;
                        d.term_counts[term][1] += 1;
                    } else {
                        d.term_counts[term] = [freq, 1, 0]; // last pos will be calculated later
                    }
                    // count total term count
                    d.total_terms++;
                    // count distinct documents
                    if (!unique_docs[doc_id]) {
                        d.total_docs++;
                        unique_docs[doc_id] = true;
                    }
                });
                // calculate the tf-idf (daily) per term
                for (var term in d.term_counts) {
                    var tf = d.term_counts[term][0] / d.total_terms,
                        idf = Math.log(d.total_docs/d.term_counts[term][1]);
                    d.term_counts[term][2] = tf * idf;
                }
                return d;
            }
            """)

def calculate_daily_summary(mongo_collection, result_collection_name, filter={}):
    '''
    Calculates the daily summary from mongo_collection and produces a document of the format:
    {
        _id: date,
        value: {
            date: date,
            total_docs: total docs in date,
            total_terms: total terms in date,
            term_counts: {
                term: [frequency, doc frequency, tfidf score],
                ...
            }
        }
    }
    Saves the results into result_collection_name.
    '''
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
    pass
