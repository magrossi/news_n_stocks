from statsmodels.tsa.stattools import grangercausalitytests, adfuller
from statsmodels.api import OLS
from kpss import kpssTest
import os.path, pickle, logging, datetime, numpy as np, pandas as pd

from news_calc_term_ts import get_tfidf_ts

# Cointegration is a statistical property of a collection (X1,X2,...,Xk) of time series variables.
# First, all of the series must be integrated of order 1 (see Order of Integration).
# Next, if a linear combination of this collection is integrated of order zero,
# then the collection is said to be co-integrated.
def are_cointegrated(x, y):
    # Step 1: regress one variable on the other
    ols_result = OLS(x, y).fit()
    # Step 2: obtain the residual
    (ols_result.resid)
    # Step 3: apply Augmented Dickey-Fuller test to see whether the residual is unit root
    # if residuals are stationary, then there is cointegration between x, y (reject NULL)
    test_stat, p_value, lag, nobs, crit_values, aic_lag = adfuller(ols_result.resid)
    return test_stat < crit_values['5%']

def difference(ts, n=1):
    return (ts - ts.shift(n)).dropna()

def is_stationary(kpss_result, alpha=0.05):
    return kpss_result[1] > alpha

def find_diff_stationary(df, max_diff=5):
    i = 0
    while i < max_diff:
        if is_stationary(kpssTest(np.asarray(df), verbose=False)):
            return (i, df, True)
        else:
            df = difference(df)
        i += 1
    return (i, df, False)

def get_stocks_caused_by(terms_panel, stock_panel, term_measure):
    # now do granger tests for all stocks
    caused_symbols = []
    for symbol in stocks.columns:
        stock_df = stocks[[symbol]][symbol]
        diff_level, stock_df_diff, converged = find_diff_stationary(stock_df)
        if converged:
            # check if integrated of the same order, if so test for cointegration
            df_align, stock_df_align = align_dataframes(df, stock_df)
            if df_diff_level == diff_level and are_cointegrated(df_align, stock_df_align):
                lag, caused = find_granger_causality(stock_df_diff, df, max_lags=3)
                if caused:
                    caused_symbols.append((symbol, lag))
            #else:
                #print 'Symbol %s has different order of integration or is not cointegrated. Skipping.' % symbol
            #else:
               #print 'Symbol %s did not converge. Skipping.' % symbol
    return caused_symbols

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')
    logging.captureWarnings(True)

    from pymongo import MongoClient
    client = MongoClient('mongodb://localhost:27017/')
    db = client['news2']
    daily_summaries = db['daily_summary']
    period_summaries = db['period_summary']

    # stock data
    sd = pd.read_pickle('D:/Study/DCU/MCM/Practicum/data/stock_panel.pkl')

    # find top n terms of the day
    daily_summary = period_summaries.find_one({
        'period_type' : { '$eq' : 'daily' },
        'period_end' : { '$eq' : base_date }
    })
    daily_relevant_terms = map(lambda x : x[0], daily_summary['term_counts'])
    relevant_terms = daily_relevant_terms[:100]
    terms_panel = get_tfidf_ts(daily_summaries, relevant_terms, {})
