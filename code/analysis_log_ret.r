require(zoo)
require(xts)
require(scales)
require(forecast)
require(tseries)
require(caret)

# read term csv
symbol <- 'KO'
symbol_term_csv <- read.csv(paste(symbol, '_aligned_log_ret_with_terms_diff.csv', sep=''), header = TRUE)
symbol_term_csv[,1] <- as.Date(symbol_term_csv[,1], format="%Y-%m-%d")

# transform into time seris
symbol_term_xts <- as.xts(symbol_term_csv[,-1], symbol_term_csv[,1])
symbol_term_xts <- scale(symbol_term_xts)
  
# split data into in sample and out of sample data
split_date = as.Date('2014-8-14')
sample = window(symbol_term_xts,end=split_date-1)
test = window(symbol_term_xts,start=split_date)
y.all = symbol_term_xts[,1]
y.sample = sample[,1]
x.sample = sample[,7]
y.test = test[,1]
x.test = test[,7]

# do arima analysis using auto.arima
y.fit <- auto.arima(y.sample, xreg=x.sample, seasonal=TRUE)
y.fit.simple <- auto.arima(y.sample)
y.fit.simple
# Plot the fitted model comparing to real data
#plot(fitted(y.fit), col="red", ylab="", main = "Fitted model vs Original data") # Fitted model
#lines(fitted(y.fit.simple), col="pink", ylab="")
#lines(as.ts(y.sample), col="blue", ylab="") # original data

# predict
y.forecast <- forecast(y.fit, h = nrow(x.test), xreg=x.test)
plot(y.forecast)

# Plot the prediction and compare it with the real values
par(yaxt="n")
plot(as.ts(y.all), ylab="", col="blue", main="Forecast")
lines(y.forecast$mean,col="red",ylab="",lty=2,lwd=2)

