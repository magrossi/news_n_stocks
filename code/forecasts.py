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

def get_arma_order(endog, exog):
    if not exog is None:
        auto_arma_order = arma_order_select_ic(endog, ic=['aic'], model_kw={'exog':exog}, fit_kw={'exog':exog,'maxiter':100})
        return auto_arma_order['aic_min_order'][0], auto_arma_order['aic_min_order'][1]
    else:
        auto_arma_order = arma_order_select_ic(endog, ic=['aic'], fit_kw={'maxiter':100})
        return auto_arma_order['aic_min_order'][0], auto_arma_order['aic_min_order'][1]

# order may not be correctly calculated
# the auto_arma_order is not reliable at all..
def get_fitted_model(endog, exog, order, transparams=False):
    try:
        if not exog is None:
            return ARIMA(endog, order, exog=exog).fit(transparams=transparams,maxiter=100)
        else:
            return ARIMA(endog, order).fit(transparams=transparams,maxiter=100)
    except Exception as e:
        (ar, i, ma) = order
        if ma > 0:
            ma -= 1
        elif ar > 0:
            ar -= 1
        else:
            return 'fail: ' + e.message

        logging.warn('auto order failed: trying to fit model with lower order...')
        return get_fitted_model(endog, exog, (ar, i, ma), transparams=transparams)

def scale_df(df):
    df -= df.min()  # equivalent to df = df - df.min()
    df /= df.max()  # equivalent to df = df / df.max()
    return df

def get_forecasts(term_panel, term_measure, stock_panel, start_date, stock_symbol, term_list):
    logging.info('start get_forecasts for %s with terms (%s)' % (stock_symbol, ','.join(term_list)))
    transparams = True # let arima model do it for us

    ks = '[' + stock_symbol + ']' # key for stock only (no exog)
    ka = ','.join(term_list)      # key for aggregate exogs
                                  # key for individual terms is the term itself

    # get endog and exog DataFrames
    endog = pd.DataFrame(stock_panel[stock_symbol])
    endog_diff_order, endog_diff, _ = find_diff_stationary(stock_panel[stock_symbol])
    exog_dic = {}
    for t in term_list:
        # difference to stationary if needed on the term DataFrame
        # as exog must be stationary, multiply by 10,000 to make it easier for
        # the optimizers as there is a considerable difference in scale between
        # stock and tf-idf weight
        if transparams:
            exog_dic[t] = tp[t][term_measure] * 10000
        else:
            _, exog_dic[t], _ = find_diff_stationary(tp[t][term_measure]  * 10000)

    # fill na of exog variables with 0.0000 as it has no score on the day
    exog = pd.DataFrame(exog_dic).fillna(0)
    # shift one day to match endog
    #exog = exog.set_index(exog.index.values + np.timedelta64(1,'D'))
    exog = exog.shift(1)

    # align endog and exog
    aligned = pd.concat([scale_df(endog),scale_df(exog)], axis=1).interpolate().dropna()

    # create train and test datasets
    train = aligned[:start_date - datetime.timedelta(days=1)]
    test = aligned[start_date:]

    # ####################### #
    # calculate ARIMAX orders #
    # ####################### #
    arima_orders = {}
    
    # 0: calculate without exog
    arma_order = get_arma_order(train[stock_symbol], None)
    arima_orders[ks] = (arma_order[0], 0, arma_order[1])

    # 1: calculate for all (endog, exog=term) pairs
    for t in term_list:
        arma_order = get_arma_order(train[stock_symbol], train[[t]])
        arima_orders[t] = (arma_order[0], 0, arma_order[1])

    # 2: calculate for (endog, exog=[terms]) pair
    if len(term_list) > 1:
        arma_order = get_arma_order(train[stock_symbol], train[term_list])
        arima_orders[ka] = (arma_order[0], 0, arma_order[1])

    # ####################### #
    # fit models              #
    # ####################### #
    models = {}
    
    # 0: calculate without exog
    models[ks] = get_fitted_model(train[stock_symbol], None, arima_orders[ks])
    
    # 1: calculate for all (endog, exog=term) pairs
    for t in term_list:
        models[t] = get_fitted_model(train[stock_symbol], train[[t]], arima_orders[t])

    # 2: calculate for (endog, exog=[terms]) pair
    if len(term_list) > 1:
        models[ka] = get_fitted_model(train[stock_symbol], train[term_list], arima_orders[ka], transparams=transparams)

    # ############################################## #
    # do forecast out-of-sample for all test dataset #
    # ############################################## #
    forecast = {}
    test_dates = [start_date + datetime.timedelta(days=x) for x in range(0, len(test))]
    for dt in test_dates:
        # 0: calculate without exog
        if not isinstance(models[ks], basestring):
        fval = models[ks].forecast(steps=1)[0]
        if ks in forecast:
            forecast[ks] = np.append(forecast[ks], fval)
        else:
            forecast[ks] = fval
                    
        # 1: calculate for all (endog, exog=term) pairs
        for t in term_list:
            # ignore models that failed to fit
            if not isinstance(models[t], basestring):
                # forecast next value
                fval = models[t].forecast(steps=1, exog=test[[t]][dt:dt])[0]
                # add to forecast array
                if t in forecast:
                    forecast[t] = np.append(forecast[t], fval)
                else:
                    forecast[t] = fval

                # add test value to model in order to predict the next day
                # using train values, this way it is more realistic as how it
                # would be done in the real world
                models[t].data.endog = np.append(models[t].data.endog, test[stock_symbol][dt:dt], axis=0)
                models[t].data.exog = np.append(models[t].data.exog, test[[t]][dt:dt], axis=0)

        # 2: calculate for (endog, exog=[terms]) pair
        if len(term_list) > 1:
            # ignore if failed fitting
            if not isinstance(models[ka], basestring):
                # forecast next value
                fval = models[ka].forecast(steps=1, exog=test[term_list][dt:dt])[0]
                # add to forecast array
                if ka in forecast:
                    forecast[ka] = np.append(forecast[ka], fval)
                else:
                    forecast[ka] = fval

                # add test value to model in order to predict the next day
                # using train values, this way it is more realistic as how it
                # would be done in the real world
                models[ka].data.endog = np.append(models[ka].data.endog, test[stock_symbol][dt:dt], axis=0)
                models[ka].data.exog = np.append(models[ka].data.exog, test[term_list][dt:dt], axis=0)

    return {
        'orders' : arima_orders,
        'models': models,
        'forecast_dates': test_dates,
        'forecasts': forecast,
        'actual': aligned[stock_symbol]
    }

    #return { stock_symbol: term_list }


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')
    logging.captureWarnings(True)

    stock_terms = pickle.load(open("D:/Study/DCU/MCM/Practicum/data/stock_terms.pkl", "rb"))
    tp = pd.read_pickle('D:/Study/DCU/MCM/Practicum/data/terms_panel.pkl')
    sd = pd.read_pickle('D:/Study/DCU/MCM/Practicum/data/stock_panel.pkl')
    start_date = datetime.datetime(2014,8,14)

    # first symbol
    for stock_symbol in stock_terms.keys():
        fname = "D:/Study/DCU/MCM/Practicum/data/"+stock_symbol+"_forecasts.pkl"
        if not os.path.isfile(fname):
            logging.info('fitting models and forecasts for stock %s' % stock_symbol)
            term_list = list(stock_terms[stock_symbol])
            term_measure = 'TF-IDF Score'
            dic = get_forecasts(tp, term_measure, sd, start_date, stock_symbol, term_list)
            pickle.dump(dic, open(fname, "wb"))

    logging.info('finished..')
