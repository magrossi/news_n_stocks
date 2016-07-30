"""
Created on Sat Jan-03-2015
@author: Deniz Turan (http://denizstij.blogspot.co.uk/)

"""
import numpy as np
import statsmodels.api as sm
from scipy.interpolate import interp1d

def kpssTest(x, regression = "LEVEL", lshort = True, verbose = True):
    """
    KPSS Test for Stationarity

    Computes the Kwiatkowski-Phillips-Schmidt-Shin (KPSS) test for the null hypothesis that x is level or trend stationary.

    Parameters
    ----------
    x : array_like, 1d
        data series
    regression : str {'LEVEL','TREND'}
        Indicates the null hypothesis and must be one of "Level" (default) or "Trend".
    lshort : bool
        a logical indicating whether the short or long version of the truncation lag parameter is used.

    Returns
    -------
    stat : float
        Test statistic
    pvalue : float
        the p-value of the test.
    usedlag : int
        Number of lags used.

    Notes
    -----
    Based on kpss.test function of tseries libraries in R.

    To estimate sigma^2 the Newey-West estimator is used. If lshort is TRUE, then the truncation lag parameter is set to trunc(3*sqrt(n)/13), otherwise trunc(10*sqrt(n)/14) is used. The p-values are interpolated from Table 1 of Kwiatkowski et al. (1992). If the computed statistic is outside the table of critical values, then a warning message is generated.

    Missing values are not handled.

    References
    ----------
    D. Kwiatkowski, P. C. B. Phillips, P. Schmidt, and Y. Shin (1992): Testing the Null Hypothesis of Stationarity against the Alternative of a Unit Root. Journal of Econometrics 54, 159--178.

    Examples
    --------
    x=numpy.random.randn(1000)  #   is level stationary
    kpssTest(x)

    y=numpy.cumsum(x)           # has unit root
    kpssTest(y)

    z=x+0.3*arange(1,len(x)+1)   # is trend stationary
    kpssTest(z,"TREND")

    """
    x = np.asarray(x,float)
    if len(x.shape)>1:
        raise ValueError("x is not an array or univariate time series")
    if regression not in ["LEVEL", "TREND"]:
        raise ValueError(("regression option %s not understood") % regression)

    n = x.shape[0]
    if regression=="TREND":
        t=range(1,n+1)
        t=sm.add_constant(t)
        res=sm.OLS(x,t).fit()
        e=res.resid
        table=[0.216, 0.176, 0.146, 0.119]
    else:
        t=np.ones(n)
        res=sm.OLS(x,t).fit()
        e=res.resid
        table=[0.739, 0.574, 0.463, 0.347]

    tablep=[0.01, 0.025, 0.05, 0.10]
    s=np.cumsum(e)
    eta=np.sum(np.power(s,2))/(np.power(n,2))
    s2 = np.sum(np.power(e,2))/n
    if lshort:
        l=np.trunc(3*np.sqrt(n)/13)
    else:
        l=np.trunc(10*np.sqrt(n)/14)
    usedlag =int(l)
    s2=R_pp_sum(e,len(e),usedlag ,s2)

    stat=eta/s2

    pvalue , msg=approx(table, tablep, stat)

    if verbose:
        print "KPSS Test for ",regression," Stationarity\n"
        print ("KPSS %s=%f" % (regression, stat))
        print ("Truncation lag parameter=%d"% usedlag )
        print ("p-value=%f"%pvalue )

        if msg is not None:
            print "\nWarning:",msg

    return ( stat, pvalue, usedlag )


def R_pp_sum (u, n, l, s):
    tmp1 = 0.0
    for i in range(1,l+1):
        tmp2 = 0.0
        for j in range(i,n):
            tmp2 += u[j]*u[j-i]
        tmp2 = tmp2*(1.0-(float(i)/((float(l)+1.0))))
        tmp1 = tmp1+tmp2

    tmp1 = tmp1/float(n)
    tmp1 = tmp1*2.0
    return s + tmp1

def approx(x,y,v):
    if (v>x[0]):
        return (y[0],"p-value smaller than printed p-value")
    if (v<x[-1]):
        return (y[-1],"p-value greater than printed p-value")

    f=interp1d(x,y)
    av=f(v)
    return (av,None)
