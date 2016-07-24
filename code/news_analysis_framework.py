# <For each stock do>
# 1.       Find important dates by PIP (perceptual important points)
# 1.       Take most relevant news articles for the day – de-noised textual data using my windowed tfidf technique to reduce importance of highly important terms in bigger time intervals.
# 2.       Granger test the highest n daily terms against the collection of stocks and output tuple with (term, stock) that passes the test.
# 3.       Use collection of terms + stock to model some test investment strategy for that day.
# 	a.       Could be: using DTW to find possible patterns (take the K latest days from stock and then DTW against all term time series and return top M most similar patterns with a threshold.
# 	b.       Simple VAR model using term time series data.
# 	c.       .. other methods (make it pluggable)
# 	d.       In all cases if more than one term detected as influencer can average out the predictions and consolidate into system’s prediction.

if __name__ == "__main__":
    #logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')
    pass
