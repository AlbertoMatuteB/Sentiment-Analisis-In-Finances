import pandas as pd
import requests
import json
import os

def get_sentiment(api_key, stock, time_from, time_to):
    """get sentiment analysis of news for a given stock"""
    #change time format to YYYYMMDDTHHMM 
    time_from = time_from.replace('-', '')
    time_to = time_to.replace('-', '')
    time_from = time_from + 'T0000'
    time_to = time_to + 'T0000'
    url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={stock}&time_from={time_from}&time_to={time_to}&limit=1000&apikey={api_key}&datatype=json'
    r = requests.get(url)
    data = r.json()
    # Create a DataFrame from the 'feed' list within the data
    df = pd.DataFrame(data["feed"])

    # Display the DataFrame
    return df

# Test with AAPL
df = get_sentiment('YOUR_API_KEY', 'AAPL', '2023-03-01', '2023-09-01')
print(df.head())
