import datetime, numpy as np, pandas as pd
from statsmodels.tsa.stattools import grangercausalitytests
from kpss import kpssTest
from stock_downloader import download_stock_history
from news_calc_term_ts import get_tfidf_ts


def difference(ts, n=1):
    return (ts - ts.shift(n)).dropna()

# Testing the Null Hypothesis of Stationarity against the Alternative of a Unit Root
def is_stationary(kpss_result, alpha=0.05):
    return kpss_result[1] > alpha

def find_diff_stationary(df, max_diff=5):
    i = 0
    while i < max_diff:
        arr = np.asarray(df)
        if is_stationary(kpssTest(arr, regression = 'LEVEL', verbose=False)) and is_stationary(kpssTest(arr, regression = 'TREND', verbose=False)):
            return (i, df, True)
        else:
            df = difference(df)
        i += 1
    return (i, df, False)

# The Null hypothesis for grangercausalitytests is that the time series in the second column, x2,
# does NOT Granger cause the time series in the first column, x1. Grange causality means that past
# values of x2 have a statistically significant effect on the current value of x1, taking past
# values of x1 into account as regressors. We reject the null hypothesis that x2 does not Granger
# cause x1 if the pvalues are below a desired size of the test.
def is_granger_caused(test_results, alpha=0.05):
    for lag, r in test_results.iteritems():
        p_values = [r[0]['lrtest'][1], r[0]['params_ftest'][1], r[0]['ssr_ftest'][1], r[0]['ssr_chi2test'][1]]
        if all(p_value < alpha for p_value in p_values):
            return (lag, True)
    return (len(test_results), False)

def find_granger_causality(x1_df, x2_df, max_lags=5):
    data = pd.concat([x1_df, x2_df], axis=1).dropna()
    lag_1_2, causes_1_2 = is_granger_caused(grangercausalitytests(data[[0, 1]], max_lags, verbose=False)) # x1 causes x2
    lag_2_1, causes_2_1 = is_granger_caused(grangercausalitytests(data[[1, 0]], max_lags, verbose=False)) # x2 causes x1
    # x2 only granger-causes x1 if, and only if, x1 does NOT cause x2 and x2 CAUSES x1
    return (lag_2_1, True if causes_2_1 and not causes_1_2 else False)

def relate_term_n_stock_ts(term_panel, stocks_panel):
    # both term_df and stock_df must be stationary
    # for all stock dfs do:

    r = grangercausalitytests(stock, max_lags, verbose=False):

    # result dictionary of terms list of stocks that are granger-caused by term_df


def convert_to_matrix(pd_df, col=0):
    i = 0
    for arr in pd_df.as_matrix():
        value = arr[0]
        yield [i, value]
        i += 1





period_summaries = db['period_summary']
query = period_summaries.aggregate([{
      '$group': {
        '_id': 'date_range',
        'min_date': { '$min': '$period_start' },
        'max_date': { '$max': '$period_end' }
      }
    }]).next()





r = grangercausalitytests(np.random.random((100,2)), 5, addconst=True, verbose=True):

example: r[1][0]['lrtest']

r = {
    1: ({'lrtest': (2.5498146592526325, 0.11030719462362751, 1),
         'params_ftest': (2.5046637818898794, 0.11679877991627048, 96.0, 1),
          'ssr_ftest': (2.5046637818898669, 0.1167987799162722, 96.0, 1),
          'ssr_chi2test': (2.5829345250739255, 0.1080212350855112, 1)},....
    2: ({'lrtest': ...
    3: ...
    4: ...
    5: ...
}
>>> r[1][0]['lrtest']
only one df variable..
(1.3813412379881242, 0.23987281780151004, 1)
>>> r[1][0]['params_ftest']
Test (F or Chi2), p-value, df_denom, df_num
(1.3488708874609374, 0.24835521552836354, 96.0, 1)

Granger Causality
('number of lags (no zero)', 1)
ssr based F test:         F=1.3489  , p=0.2484  , df_denom=96, df_num=1
ssr based chi2 test:   chi2=1.3910  , p=0.2382  , df=1
likelihood ratio test: chi2=1.3813  , p=0.2399  , df=1
parameter F test:         F=1.3489  , p=0.2484  , df_denom=96, df_num=1

Granger Causality
('number of lags (no zero)', 2)
ssr based F test:         F=0.9479  , p=0.3913  , df_denom=93, df_num=2
ssr based chi2 test:   chi2=1.9977  , p=0.3683  , df=2
likelihood ratio test: chi2=1.9776  , p=0.3720  , df=2
parameter F test:         F=0.9479  , p=0.3913  , df_denom=93, df_num=2

Granger Causality
('number of lags (no zero)', 3)
ssr based F test:         F=0.8053  , p=0.4942  , df_denom=90, df_num=3
ssr based chi2 test:   chi2=2.6038  , p=0.4568  , df=3
likelihood ratio test: chi2=2.5695  , p=0.4629  , df=3
parameter F test:         F=0.8053  , p=0.4942  , df_denom=90, df_num=3

Granger Causality
('number of lags (no zero)', 4)
ssr based F test:         F=0.6327  , p=0.6405  , df_denom=87, df_num=4
ssr based chi2 test:   chi2=2.7927  , p=0.5931  , df=4
likelihood ratio test: chi2=2.7528  , p=0.6000  , df=4
parameter F test:         F=0.6327  , p=0.6405  , df_denom=87, df_num=4

Granger Causality
('number of lags (no zero)', 5)
ssr based F test:         F=0.5537  , p=0.7351  , df_denom=84, df_num=5
ssr based chi2 test:   chi2=3.1312  , p=0.6798  , df=5
likelihood ratio test: chi2=3.0807  , p=0.6875  , df=5
parameter F test:         F=0.5537  , p=0.7351  , df_denom=84, df_num=5
