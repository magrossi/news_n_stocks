import logging, datetime, time, pandas.io.data as web

def download_stock_history(start_date, end_date, stocks, source='yahoo'):
    start_total_time = time.time()
    logging.info('downloading historical data from %s to %s for stocks %s and source %s', start_date, end_date, ', '.join(stocks), source)
    try:
        data = web.DataReader(stocks, source, start_date, end_date)
        return data
    except Exception, e:
        logging.error('error processing stock %s with error %s', stock, e.message)
        return
    finally:
        logging.info('finished processing in %f seconds', time.time() - start_total_time)

if __name__ == "__main__":
    # configure logger
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')
    # test config params
    stocks = ['MSFT', 'AA', 'GM']
    start = datetime.datetime(2013,01,01)
    end = datetime.datetime(2014,10,20)
    stock_data = download_stock_history(start, end, stocks)
