from datetime import datetime
from random import randint
from tracemalloc import Snapshot
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pycoingecko import CoinGeckoAPI
import pandas as pd
import time
from datetime import date
import snscrape.modules.twitter as sntwitter
import itertools
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import numpy as np
#from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook



def ExtractCoinGecko():
    cg = CoinGeckoAPI()
    data = cg.get_coins_markets(vs_currency='eur')
    return data

def ExtractSymbol(ti):
    data=ti.xcom_pull(task_ids='Extract_CoinGecko')
    List_crpyto=pd.DataFrame(data, columns=['symbol'])
    List_crpyto=list(List_crpyto["symbol"])
    print(List_crpyto)
    return List_crpyto[:10]

def TwitterExtractor(ti):
    data=ti.xcom_pull(task_ids='Extract_Symbol')
    export= pd.DataFrame(columns = ["symbol", "polarity"])
    for crypto in data:
        df=pd.DataFrame(itertools.islice(sntwitter.TwitterSearchScraper('"(#%s) lang:en within_time:5m"'%crypto).get_items(),50))
        df=df.drop(['url','date','id', 'username', 'outlinks', 'outlinksss','tcooutlinks', 'tcooutlinksss'], axis = 1)
        #df_week=pd.DataFrame(itertools.islice(sntwitter.TwitterSearchScraper('"(#%s) lang:en within_time:7d"'%crypto).get_items(),500))
        polar=0
        for sentence in df['content']:
            sid_obj = SentimentIntensityAnalyzer()
            sentiment_dict = sid_obj.polarity_scores(sentence)
            polar=polar+sentiment_dict['pos']
        polar=polar*100
        if polar >= 500:
            polarity="Positive"
        if polar < 500:
            polarity="Negative"
        s_row = pd.Series([crypto, polarity], index=export.columns)
        export = export.append(s_row,ignore_index=True)
    export=export.to_json()
    return export

def Aggr(ti):
    data=ti.xcom_pull(task_ids='Extract_CoinGecko')
    sentiment=ti.xcom_pull(task_ids='Extract_Twitter')
    data = pd.DataFrame(data, columns=['id','symbol','current_price','market_cap','total_volume','high_24h','low_24h','total_supply','ath','ath_date','last_updated'])
    sentiment=pd.read_json(sentiment, orient ='column')
    final=pd.merge(data,sentiment, on='symbol', how="inner")
    final=final.to_json()
    print(final)
    return final

def load(ti):
    data=ti.xcom_pull(task_ids='Aggregate')
    hook=PostgresHook(postgres_conn_id="postgres_localhost")
    hook.run('create table if not exists crypto (currency character varying PRIMARY KEY, price float(2))')
    data = pd.read_json(data, orient ='column')
    data=data.drop(data.columns.difference(['symbol','current_price']), 1, inplace=False)
    for row in data.itertuples(index=False):
        print(row)
        name=row[0]
        price=row[1]
        query = 'insert into crypto (currency, price) values (%s,%s) ON CONFLICT DO NOTHING;'
        hook=PostgresHook(postgres_conn_id="postgres_localhost")
        hook.run(query, parameters=(name, price))

        
default_args={"owner":"airflow",}
with DAG(
    dag_id="myCrypto",
    start_date=datetime(2022,5,23),
    schedule_interval='*/1 * * * *', #..every 1 minutes
    default_args=default_args)as dag:

    Extract_CoinGecko = PythonOperator(
        task_id= 'Extract_CoinGecko',
        python_callable= ExtractCoinGecko
    )

    Extract_Symbol = PythonOperator(
        task_id= 'Extract_Symbol',
        python_callable= ExtractSymbol
    )

    Extract_Twitter = PythonOperator(
        task_id= 'Extract_Twitter',
        python_callable= TwitterExtractor
    )

    Aggregate = PythonOperator(
        task_id= 'Aggregate',
        python_callable= Aggr
    )

    load_data_Postgres = PythonOperator(
        task_id= 'load_data_Postgres',
        python_callable= load
    )

  
Extract_CoinGecko >> [Aggregate,Extract_Symbol]
Extract_Symbol >> Extract_Twitter >> Aggregate >> load_data_Postgres