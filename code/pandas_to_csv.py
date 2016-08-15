from statsmodels.tsa.arima_model import ARIMA
from statsmodels.tsa.stattools import arma_order_select_ic
from kpss import kpssTest
import os.path, pickle, logging, datetime, numpy as np, pandas as pd

def difference(ts, n=1):
    return (ts - ts.shift(n)).dropna()

def is_stationary(kpss_result, alpha=0.05):
    return kpss_result[1] > alpha

def find_diff_stationary(df, max_diff=5):
    i = 0
    while i < max_diff:
        if is_stationary(kpssTest(df, verbose=False)):
            return (i, df, True)
        else:
            df = difference(df)
        i += 1
    return (i, df, False)

def save_to_csv(term_panel, term_measure, stock_panel, stock_symbol, term_list):
    logging.info('saving data for %s and %s' % (stock_symbol, ','.join(term_list)))

    # get endog and exog DataFrames
    endog = pd.DataFrame(stock_panel[stock_symbol])
    endog_log_ret = np.log(endog) - np.log(endog.shift(1))

    exog_dic = {}
    exog_dic_diff = {}
    for t in term_list:
        exog_dic[t] = tp[t][term_measure]
        _, exog_dic_diff[t], _ = find_diff_stationary(tp[t][term_measure])

    # fill na of exog variables with 0.0000 as it has no score on the day
    exog = pd.DataFrame(exog_dic).fillna(0)
    exog_diff = pd.DataFrame(exog_dic_diff).fillna(0)

    # shift one day to match endog
    exog = exog.shift(1)
    exog_diff = exog_diff.shift(1)

    # align endog and exog
    aligned = pd.concat([endog,exog], axis=1).interpolate().dropna()
    aligned_diff = pd.concat([endog,exog_diff], axis=1).interpolate().dropna()

    aligned_log_ret = pd.concat([endog_log_ret,exog], axis=1).interpolate().dropna()
    aligned_log_ret_diff = pd.concat([endog_log_ret,exog_diff], axis=1).interpolate().dropna()

    # save to csv so R can read it
    aligned.to_csv('D:/Study/DCU/MCM/Practicum/data/' + stock_symbol + '_aligned_with_terms.csv')
    aligned_diff.to_csv('D:/Study/DCU/MCM/Practicum/data/' + stock_symbol + '_aligned_with_terms_diff.csv')
    aligned_log_ret.to_csv('D:/Study/DCU/MCM/Practicum/data/' + stock_symbol + '_aligned_log_ret_with_terms.csv')
    aligned_log_ret_diff.to_csv('D:/Study/DCU/MCM/Practicum/data/' + stock_symbol + '_aligned_log_ret_with_terms_diff.csv')

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')
    logging.captureWarnings(True)

    stock_terms = pickle.load(open("D:/Study/DCU/MCM/Practicum/data/stock_terms.pkl", "rb"))
    tp = pd.read_pickle('D:/Study/DCU/MCM/Practicum/data/terms_panel.pkl')
    sd = pd.read_pickle('D:/Study/DCU/MCM/Practicum/data/stock_panel.pkl')
    term_measure = 'TF-IDF Score'

    # first symbol
    for stock_symbol in stock_terms.keys():
        logging.info('saving stock %s and related words to csv..' % stock_symbol)
        term_list = list(stock_terms[stock_symbol])
        save_to_csv(tp, term_measure, sd, stock_symbol, term_list)

    logging.info('finished..')
