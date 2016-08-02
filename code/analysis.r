require(zoo)
require(xts)
require(scales)
require(forecast)
require(tseries)
require(caret)

# read term csv
bank <- read.csv('term_bank_ts.csv', header = FALSE)
bank_df <- data.frame(date=as.Date(bank[, 1], format="%Y-%m-%d"), value=bank[, 2])
bank_ts <- ts(bank_df$value, start=min(bank_df$date), frequency = 1)

# read stock csv
stock <- read.csv('stock_cvx_ts.csv', header = FALSE)
stock_df <- data.frame(date=as.Date(stock[, 1], format="%Y-%m-%d"), value=stock[, 2])
stock_ts <- ts(stock_df$value, start=min(stock_df$date), frequency = 1)

# intersect data
data <- ts.intersect(bank_ts, stock_ts)
data[is.na(data)] <- 0 # replace Na with 0 (should have been dealt with earlier)

# split data into in sample and out of sample data
n_test_obs = 5
split_date = tsp(data)[2]-(n_test_obs-1)/frequency(data)
sample = window(data,end=split_date-1)
test = window(data,start=split_date)
y.sample = sample[,2]
x.sample = sample[,1]
y.test = test[,2]
x.test = test[,1]

# do arima analysis
arima_coeff <- auto.arima(y.sample, xreg=x.sample)
# arima_coeff (0,1,0)
y.fit <- Arima(y.sample, xreg=x.sample, order = c(1,1,0))

# Plot the fitted model comparing to real data
plot(fitted(y.fit), ylab="", type="o", lwd=1.5, lty=2, main = "Fitted model vs Original data") # Fitted model
lines(y.sample, col=4, type="o", ylab="") # original data

# predict
y.forecast <- forecast(y.fit, h = nrow(x.test), xreg=x.test)

# Plot the prediction and compare it with the real values
par(yaxt="n")
plot(y.sample, ylab="", type="o", main="Forecast")
lines(y.forecast$mean,col=4,ylab="",lty=2,lwd=2,type="o")
