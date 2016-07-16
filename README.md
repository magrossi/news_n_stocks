# News n' Stocks

Source code for experiments on analyzing news articles to help in the stock market decision making. This code is meant to be run against a dataset of news articles saved in JSON format that follow the below format:

```json
{  
    "author": "author name",
    "date": "date in YYYY-MM-DD format",
    "text": "full article text pre-cleaned",
    "title": "title of article",
    "url": "url"
}
```

## Converting raw news files to MongoDB

### Pre-process documents into database

Just import the process_items and InputItem helper from the *news_preprocessor.py* file.
```python
import logging
from news_preprocessor import process_items, InputItem
from pymongo import MongoClient

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

input_dir = 'your_input_dir'
ngram_size = 1 # generate word ngrams up to ngram_size
filename_regex = '(?i).*(\.json)$'
date_format = '%Y-%m-%d'
source = 'nytimes' # example of source metadata

mongo_server = 'mongodb://localhost:27017/'
db_name = 'news' # name of mongo database
collection_name = 'docs' # name of mongo collection to save into

client = MongoClient(mongo_server)
db = client[db_name]
coll = db[collection_name]

# this is just a helper for my own directory structure
# folders_tags = [('world', 'world')]
# folders_tags = [('us', 'us'), ('U.S', 'us')]
# folders_tags = [('politics', 'politics')]
# folders_tags = [('BusinessDay', 'business'), ('Business Day', 'business')]
folders_tags = [('business', 'business')]

input_items = []
for idx, (folder, tag) in enumerate(folders_tags):
  input_items.append(InputItem(dir=input_dir+folder, source=source, tags=[tag], date_format=date_format, file_match_regex=filename_regex))

# inserts documents into MongoDB after insert_after documents have been produced in memory
process_items(input_items, coll, ngram_size=ngram_size, insert_after=1000)

```
This will save the documents into a MongoDB collection in the format below.
```json
{
    "source": "source of news article",
    "date": "date in the format YYYY-MM-DD",
    "tags": ["list", "of", "tags"],
    "title": "title of news article",
    "url": "url of article",
    "bag_of_words": [["ngram", ngram_frequency],..]
}
```

### Process documents into daily summary database

Uses TF-IDF weighing to convert each bag of words documents into daily aggregated term weighed documents for easy converting to time series of terms and further processing.

```python
import logging, datetime
from pymongo import MongoClient
from news_calc_daily_summary import calculate_daily_summary

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

mongo_server = 'mongodb://localhost:27017/'
db_name = 'news' # name of mongo database
collection_name = 'docs' # name of mongo collection to save into
mr_collection_name = 'daily_summary' # name of mongo collection to output daily summaries into

client = MongoClient(mongo_server)
db = client[db_name]
coll = db[collection_name]

calculate_daily_summary(coll, mr_collection_name, filter={}) # you can inform mongo-like filter here
```

### Consolidate Daily Summaries into Period Summaries (daily)

###### First check available dates on your database to facilitate the process
```js
db.daily_summary.aggregate(
  [{
      $group: {
        _id: "AllSummaries",
        min_date: { $min: "$value.date" },
        max_date: { $max: "$value.date" }
      }
    }])
```
What should return something like the below.
```json
{
    "_id" : "AllSummaries",
    "min_date" : ISODate("2013-01-01T00:00:00Z"),
    "max_date" : ISODate("2014-10-19T00:00:00Z")
}
```
Then use the dates to calculate the daily period summaries. Any period summary can be calculated. Just make sure to set the period tag accordingly.

```python
import logging, datetime
from news_calc_period_summary import calculate_period_summary
from pymongo import MongoClient

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

mongo_server = 'mongodb://localhost:27017/'
db_name = 'news'
collection_name = 'daily_summary'
out_collection_name = 'period_summary'

client = MongoClient(mongo_server)
db = client[db_name]
coll = db[collection_name]
out_coll = db[out_collection_name]

start = datetime.datetime(2013,01,01)
end = datetime.datetime(2014,10,20)
date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days)]

for date in date_generated:
    calculate_period_summary(coll, out_coll, 'daily', 1000, {'_id': { '$eq': date }})
```

### Generate time series data for any terms (ngram)
The resulting time series will indicate the terms varying importance in your corpus over time. The results will be a dictionary of the input terms and for each key an array of data points with ```[date, term frequency, document frequency, tf-idf score]```.

```python
import logging
from news_calc_term_ts import get_tfidf_ts

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')

from pymongo import MongoClient
mongo_server = 'mongodb://localhost:27017/'
db_name = 'news'
collection_name = 'daily_summary'
filter = {}

client = MongoClient(mongo_server)
db = client[db_name]
coll = db[collection_name]

terms = ['night', 'day']

logging.debug('testing get_tfidf_ts function for terms %s', ', '.join(terms))
ts = get_tfidf_ts(coll, terms, filter)
logging.debug(ts)
```
