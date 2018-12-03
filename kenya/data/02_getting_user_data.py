import json
import os.path as osp

import pandas as pd
from tqdm import tqdm

import tweepy

CURRENT_PATH = osp.dirname(osp.realpath(__file__))
DATA_DIR = osp.join(CURRENT_PATH, '..', '..', 'data')

users = pd.read_csv(osp.join(DATA_DIR, 'interim',
                             'users_popular.txt'), header=None, names=['user_id'])
users_church = pd.read_csv(osp.join(
    DATA_DIR, 'interim', 'users_church.txt'), header=None, names=['user_id'])
users = pd.concat([users, users_church])

cred_path = osp.join(DATA_DIR, 'external', 'twitter_creds.json')
with open(cred_path) as f:
    twitter_creds_list = list(json.load(f).values())

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

cities = ['Kenya', 'Nairobi', 'Mombasa', 'Nakuru',
          'Eldoret', 'Kisumu', 'Thika', 'Kitale', 'Malindi']
user_ids = list(users['user_id'])
users_info = {}

for start in tqdm(range(0, len(user_ids), 100)):
    end = start + 100
    users_to_parse = [
        user for user in user_ids[start:end] if user not in users_info
    ]
    cur_users_info = api.lookup_users(users_to_parse, include_entities=False)
    for user in cur_users_info:
        if user.location and any(city in user.location for city in cities):
            users_info[user.id] = {
                'name': user.name,
                'screen_name': user.screen_name,
                'location': user.location,
                'description': user.description,
                'followers_count': user.followers_count,
                'friends_count': user.friends_count,
                'statuses_count': user.statuses_count
            }
    if start % 10000 == 0:
        with open(osp.join(DATA_DIR, 'interim', 'users_info.json'), 'w') as fout:
            json.dump(users_info, fout)
        print(start)
with open(osp.join(DATA_DIR, 'interim', 'users_info.json'), 'w') as fout:
    json.dump(users_info, fout)
