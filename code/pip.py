from fastpip import pip
import matplotlib.pyplot as plt

def _pandas_to_xy_array(pd_df):
    xy = []
    i = 0
    for val in pd_df.as_matrix():
        xy.append([i, val])
        i += 1
    return xy

def _pip_array_to_dates(pd_df, pip_array):
    dates = []
    for date_index, value in pip_array:
        dates.append(pd_df.index[date_index])
    return dates

def get_pip_dates(df, k):
    return _pip_array_to_dates(df, pip(_pandas_to_xy_array(df), k))

if __name__ == "__main__":
    import datetime, pandas as pd, pandas.io.data as web, matplotlib, matplotlib.pyplot as plt
    matplotlib.style.use('ggplot')
    data = web.DataReader(['IBM'], 'yahoo', datetime.datetime(2013,01,01), datetime.datetime(2014,01,01))
    df = data.ix['Close',:,'IBM']
    dates = get_pip_dates(df, 15)
    # plot
    df.plot()
    plt.plot_date(x=dates, y=df[dates])
    plt.show()
