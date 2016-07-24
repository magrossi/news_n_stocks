from statsmodels.tsa.stattools import grangercausalitytests
import numpy as np
from fastpip import pip

def convert_to_matrix(pd_df, col=0):
    i = 0
    for arr in pd_df.as_matrix():
        value = arr[0]
        yield [i, value]
        i += 1

pip(convert_to_matrix(df), 10)

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
