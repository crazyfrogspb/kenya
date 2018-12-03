import glob
import json
import os.path as osp

import numpy as np
import pandas as pd

CURRENT_PATH = osp.dirname(osp.realpath(__file__))
DATA_DIR = osp.join(CURRENT_PATH, '..', '..', 'data')

with open('data/users_info.json', 'r') as fin:
    users_info = json.load(fin)

users_df = pd.DataFrame(users_info).T.reset_index()
users_df.rename({'index': 'user_id'}, axis=1, inplace=True)
users_df.to_csv(osp.join(DATA_DIR, 'interim', 'kenya_users.csv'), index=False)
users_df = users_df.loc[users_df.statuses_count > 0]
users_df.to_csv(osp.join(DATA_DIR, 'interim',
                         'kenya_users_1_tweet.csv'), index=False)

church_terms = ['catholic', 'pentecostal', 'anglican', 'methodist',
                'lutheran', 'presbyterian', 'baptist', 'charismatic', 'born again']
priest_terms = ['pastor', 'reverend', 'priest']


users_df['user_id'] = pd.to_numeric(users_df['user_id'])
users_df.drop_duplicates('user_id', inplace=True)

church_files = glob.glob(osp.join(DATA_DIR, 'interim', 'church_txt', '*.txt'))
for church_file in church_files:
    with open(church_file, 'r') as fin:
        users = fin.read().split()
    users = [int(user) for user in users]
    acc_name = osp.splitext(osp.basename(church_file))[0]
    users_df[f'follows_{acc_name}'] = np.where(
        users_df.user_id.isin(users), 1, 0)
users_df['follows_at_least_one'] = np.where(
    users_df.filter(regex='follows_').sum(axis=1) > 0, 1, 0)

users_df = users_df.loc[(users_df.description.fillna('').str.lower().str.contains(
    '|'.join(church_terms))) | users_df.follows_at_least_one == 1]
users_df = users_df.loc[~users_df.description.fillna(
    '').str.lower().str.contains('|'.join(priest_terms))]

for term in church_terms:
    users_df[f'contains_{term}'] = np.where(
        users_df.description.fillna('').str.lower().str.contains(term), 1, 0)

users_df['contains_at_least_one'] = np.where(
    users_df.filter(regex='contains_').sum(axis=1) > 0, 1, 0)
users_df = users_df.sample(frac=1.0, random_state=24)

users_df.to_csv(osp.join(DATA_DIR, 'processed', 'all_users.csv', index=False))
