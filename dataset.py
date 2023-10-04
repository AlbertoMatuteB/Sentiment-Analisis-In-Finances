import pandas as pd
import requests
import json
import os

def get_sentiment(api_key, stock, time_from, time_to):
    """get sentiment analysis of news for a given stock"""
    # change time format to YYYYMMDDTHHMM 
    time_from = time_from.replace('-', '')
    time_to = time_to.replace('-', '')
    time_from = time_from + 'T0000'
    time_to = time_to + 'T0000'
    url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={stock}&time_from={time_from}&time_to={time_to}&limit=1000&apikey={api_key}&datatype=json'
    r = requests.get(url)
    data = r.json()
    time_published_column = []
    overall_sentiment_score_column = []
    # Extract the relevant data from the JSON response
    for values in data["feed"]:
        time_published_column.append(values["time_published"])
        overall_sentiment_score_column.append(float(values["overall_sentiment_score"]))

    #change time_published to datetime
    time_published_column = pd.to_datetime(time_published_column, format='%Y-%m-%d')

    # remove time from time_published_column
    time_published_column = time_published_column.date

    # Create the DataFrame
    df = pd.DataFrame({
        "time_published": time_published_column,
        "overall_sentiment_score": overall_sentiment_score_column
    })
    return df

def get_daily_time_series(api_key, stock, time_from, time_to):
    """get daily time series for a given stock"""
    # change time format to YYYYMMDD
    time_from = time_from.replace('-', '')
    time_to = time_to.replace('-', '')
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock}&apikey={api_key}&datatype=json'
    r = requests.get(url)
    data = r.json()
    # Extract the relevant data from the JSON response
    date_column = []
    open_column = []
    high_column = []
    low_column = []
    close_column = []
    volume_column = []

    for date, values in data["Time Series (Daily)"].items():
        date_column.append(date)
        open_column.append(float(values["1. open"]))
        high_column.append(float(values["2. high"]))
        low_column.append(float(values["3. low"]))
        close_column.append(float(values["4. close"]))
        volume_column.append(int(values["5. volume"]))

    # Create the DataFrame
    df = pd.DataFrame({
        "Date": date_column,
        "Open": open_column,
        "High": high_column,
        "Low": low_column,
        "Close": close_column,
        "Volume": volume_column
    })
    
    return df

def get_weekly_time_series(api_key, stock, time_from, time_to):
    """get daily time series for a given stock"""
    # change time format to YYYYMMDD
    time_from = time_from.replace('-', '')
    time_to = time_to.replace('-', '')
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol={stock}&apikey={api_key}&datatype=json'
    r = requests.get(url)
    data = r.json()
    # Extract the relevant data from the JSON response
    date_column = []
    open_column = []
    high_column = []
    low_column = []
    close_column = []
    volume_column = []

    for date, values in data["Weekly Time Series"].items():
        date_column.append(date)
        open_column.append(float(values["1. open"]))
        high_column.append(float(values["2. high"]))
        low_column.append(float(values["3. low"]))
        close_column.append(float(values["4. close"]))
        volume_column.append(int(values["5. volume"]))

    # Create the DataFrame
    df = pd.DataFrame({
        "Date": date_column,
        "Open": open_column,
        "High": high_column,
        "Low": low_column,
        "Close": close_column,
        "Volume": volume_column
    })
    
    return df

def get_monthly_time_series(api_key, stock, time_from, time_to):
    """get daily time series for a given stock"""
    # change time format to YYYYMMDD
    time_from = time_from.replace('-', '')
    time_to = time_to.replace('-', '')
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol={stock}&apikey={api_key}&datatype=json'
    r = requests.get(url)
    data = r.json()
    # Extract the relevant data from the JSON response
    date_column = []
    open_column = []
    high_column = []
    low_column = []
    close_column = []
    volume_column = []

    for date, values in data["Monthly Time Series"].items():
        date_column.append(date)
        open_column.append(float(values["1. open"]))
        high_column.append(float(values["2. high"]))
        low_column.append(float(values["3. low"]))
        close_column.append(float(values["4. close"]))
        volume_column.append(int(values["5. volume"]))

    # Create the DataFrame
    df = pd.DataFrame({
        "Date": date_column,
        "Open": open_column,
        "High": high_column,
        "Low": low_column,
        "Close": close_column,
        "Volume": volume_column
    })
    
    return df

# create a csv file for sentiment data as a feature and the time series columns as prossible targets for the classification model
# using 5 different stocks (APPLE, NVIDIA, JOHNSON & JOHNSON, EXXON, AND JPMORGAN)
# retrieve data for dates between 2023-01-01 and 2023-06-01
# create a csv file for each stock and each time series function

# create a list of stocks
stocks = ['AAPL', 
          #'NVDA', 
          #'JNJ', 
          #'XOM', 
          #'JPM'
          ]

for stock in stocks:
    sentiment_df = get_sentiment('U7G77Q1UMNIYMSP1', stock, '2023-01-01', '2023-06-01')
    daily_df = get_daily_time_series('U7G77Q1UMNIYMSP1', stock, '2023-01-01', '2023-06-01')
    weekly_df = get_weekly_time_series('U7G77Q1UMNIYMSP1', stock, '2023-01-01', '2023-06-01')
    monthly_df = get_monthly_time_series('U7G77Q1UMNIYMSP1', stock, '2023-01-01', '2023-06-01')
    
    # Print the number of rows in sentiment_df and daily_df for debugging
    print(f"Sentiment DataFrame Rows: {len(sentiment_df)}")
    print(f"Daily DataFrame Rows: {len(daily_df)}")
    
    # Join each time series dataframe with the sentiment dataframe on the date column using an outer join
    daily_df = daily_df.merge(sentiment_df, how='outer', left_on='Date', right_on='time_published')
    weekly_df = weekly_df.merge(sentiment_df, how='outer', left_on='Date', right_on='time_published')
    monthly_df = monthly_df.merge(sentiment_df, how='outer', left_on='Date', right_on='time_published')
    
    # Print the number of rows in the merged dataframes for debugging
    print(f"Merged Daily DataFrame Rows: {len(daily_df)}")
    print(f"Merged Weekly DataFrame Rows: {len(weekly_df)}")
    print(f"Merged Monthly DataFrame Rows: {len(monthly_df)}")
    
    # Drop the time_published column
    daily_df = daily_df.drop(['time_published'], axis=1)
    weekly_df = weekly_df.drop(['time_published'], axis=1)
    monthly_df = monthly_df.drop(['time_published'], axis=1)
    
    # Save the dataframes to csv files
    daily_df.to_csv(f'{stock}_daily.csv', index=False)
    weekly_df.to_csv(f'{stock}_weekly.csv', index=False)
    monthly_df.to_csv(f'{stock}_monthly.csv', index=False)


    