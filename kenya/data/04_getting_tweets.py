import datetime
import json
import os.path as osp
from random import shuffle

import pandas as pd

import tweepy
from redditscore import get_twitter_data as gtd

CURRENT_PATH = osp.dirname(osp.realpath(__file__))
DATA_DIR = osp.join(CURRENT_PATH, '..', '..', 'data')

users_combined = pd.read_csv(osp.join(
    DATA_DIR, 'processed', 'all_users.csv'))

twitter_handles = list(users_combined.screen_name.unique())

cred_path = osp.join(DATA_DIR, 'external', 'twitter_creds.json')
with open(cred_path) as f:
    twitter_creds_list = list(json.load(f).values())
twitter_creds = twitter_creds_list[0]

auths = []
for creds in twitter_creds_list:
    auth = tweepy.OAuthHandler(creds['consumer_key'], creds['consumer_secret'])
    auth.set_access_token(creds['access_key'], creds['access_secret'])
    auths.append(auth)
api = tweepy.API(
    auths,
    retry_count=3,
    retry_delay=5,
    retry_errors=set([401, 404, 500, 503]),
    monitor_rate_limit=True,
    wait_on_rate_limit=True,
    wait_on_rate_limit_notify=True)


tweets_file = osp.join(DATA_DIR, 'processed', 'tweets.csv')

start_date = datetime.date(year=2013, month=3, day=1)
if osp.isfile(tweets_file):
    tweets = pd.read_csv(tweets_file, lineterminator='\n',
                         usecols=['screen_name'])
    parsed_handles = list(tweets['screen_name'].unique())
    del tweets
else:
    parsed_handles = []
dfs = []
shuffle(twitter_handles)
for twitter_handle in twitter_handles:
    if twitter_handle in parsed_handles or pd.isnull(twitter_handle):
        continue
    try:
        if api.get_user(twitter_handle).statuses_count > 20000:
            print(
                f'User {twitter_handle} has more than 20000 tweets, skipping to save time')
            continue
        df = gtd.grab_tweets(
            twitter_creds,
            screen_name=twitter_handle,
            timeout=1.0,
            get_more=True,
            start_date=start_date,
            browser='Chrome',
            fields=['retweet_count', 'favorite_count'])
    except Exception as e:
        print(e)
        continue
    if osp.isfile(tweets_file):
        with open(tweets_file, 'a') as f:
            df.to_csv(f, header=False, index=False)
    else:
        df.to_csv(tweets_file, index=False)
    parsed_handles.append(twitter_handle)
