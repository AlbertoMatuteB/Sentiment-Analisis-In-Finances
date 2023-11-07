import pandas as pd
import requests
import json
import os
from datetime import datetime, timedelta

def get_sentiment(api_key, stock, time_from, time_to):
    """get sentiment analysis of news for a given stock"""
    # change time format to YYYYMMDDTHHMM 
    time_from = time_from.replace('-', '')
    time_to = time_to.replace('-', '')
    time_from = time_from + 'T0000'
    time_to = time_to + 'T0000'
    url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={stock}&time_from={time_from}&time_to={time_to}&limit=1000&apikey={api_key}&datatype=json'
    r = requests.get(url)
    print(url)
    data = r.json()
    # json schema
    '''{
            "title": "",
            "url": "",
            "time_published": "",
            "authors": [
                ""
            ],
            "summary": "",
            "banner_image": "",
            "source": "",
            "category_within_source": "",
            "source_domain": "",
            "topics": [
                {
                    "topic": "",
                    "relevance_score": ""
                },
            ],
            "overall_sentiment_score": ,
            "overall_sentiment_label": "",
            "ticker_sentiment": [
                {
                    "ticker": "",
                    "relevance_score": "",
                    "ticker_sentiment_score": "",
                    "ticker_sentiment_label": ""
                }
            ]
        },
    '''
    # Extract the relevant data from the JSON response
    title_column = []
    url_column = []
    time_published_column = []
    authors_column = []
    summary_column = []
    banner_image_column = []
    source_column = []
    category_within_source_column = []
    source_domain_column = []
    topics_column = []
    overall_sentiment_score_column = []
    overall_sentiment_label_column = []
    ticker_sentiment_column = []

    for news in data["feed"]:
        title_column.append(news["title"])
        url_column.append(news["url"])
        time_published_column.append(news["time_published"])
        authors_column.append(news["authors"])
        summary_column.append(news["summary"])
        banner_image_column.append(news["banner_image"])
        source_column.append(news["source"])
        category_within_source_column.append(news["category_within_source"])
        source_domain_column.append(news["source_domain"])
        topics_column.append(news["topics"])
        overall_sentiment_score_column.append(news["overall_sentiment_score"])
        overall_sentiment_label_column.append(news["overall_sentiment_label"])
        ticker_sentiment_column.append(news["ticker_sentiment"])

    # Create the DataFrame
    df = pd.DataFrame({
        "Title": title_column,
        "URL": url_column,
        "Time Published": time_published_column,
        "Authors": authors_column,
        "Summary": summary_column,
        "Banner Image": banner_image_column,
        "Source": source_column,
        "Category Within Source": category_within_source_column,
        "Source Domain": source_domain_column,
        "Topics": topics_column,
        "Overall Sentiment Score": overall_sentiment_score_column,
        "Overall Sentiment Label": overall_sentiment_label_column,
        "Ticker Sentiment": ticker_sentiment_column
    })

    return df
    


def get_daily_time_series(api_key, stock, time_from, time_to):
    """get daily time series for a given stock"""
    # change time format to YYYYMMDD
    time_from = time_from.replace('-', '')
    time_to = time_to.replace('-', '')
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock}&outputsize=full&apikey={api_key}&datatype=json'
    print(url)
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
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol={stock}&outputsize=full&apikey={api_key}&datatype=json'
    print(url)
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
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol={stock}&outputsize=full&apikey={api_key}&datatype=json'
    r = requests.get(url)
    print(url)
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


'''aapl_daily = get_daily_time_series('APIKE', 'AAPL', '2023-01-01', '2023-06-01')
aapl_daily.to_csv('aapl_daily.csv', index=False)

aapl_weekly = get_weekly_time_series('APIKE', 'AAPL', '2023-01-01', '2023-06-01')
aapl_weekly.to_csv('aapl_weekly.csv', index=False)

aapl_monthly = get_monthly_time_series('APIKE', 'AAPL', '2023-01-01', '2023-06-01')
aapl_monthly.to_csv('aapl_monthly.csv', index=False)
'''
'''
nvda_daily = get_daily_time_series('APIKE', 'NVDA', '2023-01-01', '2023-06-01')
nvda_daily.to_csv('nvda_daily.csv', index=False)

nvda_weekly = get_weekly_time_series('APIKE', 'NVDA', '2023-01-01', '2023-06-01')
nvda_weekly.to_csv('nvda_weekly.csv', index=False)

nvda_monthly = get_monthly_time_series('APIKE', 'NVDA', '2023-01-01', '2023-06-01')
nvda_monthly.to_csv('nvda_monthly.csv', index=False)
'''

'''
jnj_daily = get_daily_time_series('APIKE', 'JNJ', '2023-01-01', '2023-06-01')
jnj_daily.to_csv('jnj_daily.csv', index=False)

jnj_weekly = get_weekly_time_series('APIKE', 'JNJ', '2023-01-01', '2023-06-01')
jnj_weekly.to_csv('jnj_weekly.csv', index=False)

jnj_monthly = get_monthly_time_series('APIKE', 'JNJ', '2023-01-01', '2023-06-01')
jnj_monthly.to_csv('jnj_monthly.csv', index=False)
'''

'''xom_daily = get_daily_time_series('APIKE', 'XOM', '2023-01-01', '2023-06-01')
xom_daily.to_csv('xom_daily.csv', index=False)

xom_weekly = get_weekly_time_series('APIKE', 'XOM', '2023-01-01', '2023-06-01')
xom_weekly.to_csv('xom_weekly.csv', index=False)

xom_monthly = get_monthly_time_series('APIKE', 'XOM', '2023-01-01', '2023-06-01')
xom_monthly.to_csv('xom_monthly.csv', index=False)
'''


'''jpm_daily = get_daily_time_series('APIKE', 'JPM', '2023-01-01', '2023-06-01')
jpm_daily.to_csv('jpm_daily.csv', index=False)

jpm_weekly = get_weekly_time_series('APIKE', 'JPM', '2023-01-01', '2023-06-01')
jpm_weekly.to_csv('jpm_weekly.csv', index=False)

jpm_monthly = get_monthly_time_series('APIKE', 'JPM', '2023-01-01', '2023-06-01')
jpm_monthly.to_csv('jpm_monthly.csv', index=False)
'''

"""start_date = '2023-02-01'
end_date = '2023-04-30'

aapl_sentiment = get_sentiment('APIKE', 'AAPL', start_date, end_date)
aapl_sentiment.to_csv('aapl_sentiment_2.csv', index=False)


nvda_sentiment = get_sentiment('APIKE', 'NVDA', start_date, end_date)
nvda_sentiment.to_csv('nvda_sentiment_2.csv', index=False)

jnj_sentiment = get_sentiment('APIKE', 'JNJ', start_date, end_date)
jnj_sentiment.to_csv('jnj_sentiment_2.csv', index=False)

xom_sentiment = get_sentiment('APIKE', 'XOM', start_date, end_date)
xom_sentiment.to_csv('xom_sentiment_2.csv', index=False)

jpm_sentiment = get_sentiment('APIKE', 'JPM', start_date, end_date)
jpm_sentiment.to_csv('jpm_sentiment_2.csv', index=False)"""


# create a list of stocks
stocks = [#'AAPL', 
          #'NVDA', 
          #'JNJ', 
          #'XOM', 
          #'JPM'
          ]

def generate_monthly_dates(start_date, end_date):
    """Generate a list of month start and end dates for a given date range."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    dates = []

    while start < end:
        # Find the last day of the current month
        month_end = (start.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        if month_end > end:
            month_end = end
        dates.append((start.strftime("%Y-%m-%d"), month_end.strftime("%Y-%m-%d")))
        start = month_end + timedelta(days=1)

    return dates

# for each stock, retrieve monthly sentiment data and store them in one big csv file for each stock
for stock in stocks:
    monthly_dates = generate_monthly_dates('2022-06-01', '2023-10-01')
    monthly_sentiment = pd.DataFrame()
    for date in monthly_dates:
        start_date = date[0]
        end_date = date[1]
        sentiment = get_sentiment('APIKE', stock, start_date, end_date)
        monthly_sentiment = monthly_sentiment.append(sentiment)
    monthly_sentiment.to_csv(f'{stock}_monthly_sentiment2.csv', index=False)

