import logging, time, datetime
from bson.code import Code
from bson.son import SON

def _mapper():
    return Code("""
            function () {
                var key = this.date,
                    doc = {};
                doc[this._id] = true;
                this.bag_of_words.forEach(function (w) {
                    var term = {};
                    term[w[0]] = [w[1], 1];
                    emit(key, { terms : term, docs: doc });
                });
            }
            """)

def _reducer():
    return Code("""
            function (dt, values) {
                var a = values[0],
                    b;
                for (var i = 1; i < values.length; i++) {
                    b = values[i];
                    // aggregate the terms
                    Object.keys(b.terms).forEach(function(k) {
                        if (k in a.terms) {
                            a.terms[k][0] += b.terms[k][0];
                            a.terms[k][1] += b.terms[k][1];
                        } else {
                            a.terms[k] = b.terms[k];
                        }

                    });
                    // aggregate the docs
                    Object.keys(b.docs).forEach(function(k) {
                        if (!(k in a.docs)) {
                            a.docs[k] = true;
                        }
                    });
                }
                return a;
            }
            """)

def _finalizer():
    return Code("""
            function (dt, summary) {
                var d = {
                        date: dt,
                        total_docs: Object.keys(summary.docs).length,
                        total_terms: Object.keys(summary.terms).length,
                        term_counts: summary.terms
                    };
                // calculate the tf-idf (daily) per term
                for (var key in d.term_counts) {
                    var tf = d.term_counts[key][0] / d.total_terms,
                        idf = Math.log(d.total_docs/d.term_counts[key][1]);
                    d.term_counts[key].push(tf * idf);
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
    result = mongo_collection.map_reduce(_mapper(), _reducer(), out=SON([('merge', result_collection_name),]), query=filter, finalize=_finalizer(), full_response=True)
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
