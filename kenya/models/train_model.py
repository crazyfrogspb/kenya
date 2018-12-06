import os.path as osp

import pandas as pd
from redditscore.tokenizer import CrazyTokenizer
from tqdm import tqdm

from gensim.models import Word2Vec
from guess_language import guess_language

CURRENT_PATH = osp.dirname(osp.realpath(__file__))
DATA_DIR = osp.join(CURRENT_PATH, '..', '..', 'data')

tweets_file = osp.join(DATA_DIR, 'processed', 'tweets.csv')
tweets = pd.read_csv(
    tweets_file,
    lineterminator='\n',
    error_bad_lines=False)

tweets = tweets.sample(n=1000000, random_state=24)

tokenizer = CrazyTokenizer(keepcaps=False, ignore_stopwords='english', twitter_handles='',
                           hashtags='split', subreddits='', reddit_usernames='', emails='', urls='')

tokens = []
for i in tqdm(range(tweets.shape[0])):
    if i < len(tokens):
        continue
    current_tokens = tokenizer.tokenize(
        tweets.iloc[i, tweets.columns.get_loc('text')])
    tokens.append(current_tokens)
tweets['tokens'] = tokens
del tokens

tweets = tweets.loc[tweets.tokens.str.len() > 1, :]

tweets['gl'] = tweets.text.apply(guess_language)
tweets = tweets.loc[tweets.gl == 'en', :]

w2v_model = Word2Vec(tweets['tokens'], min_count=1, iter=10)
w2v_model.save(osp.join(CURRENT_PATH, '..', '..', 'models', 'w2v2.model'))
