import pandas as pd
import pickle
import os
from sklearn.feature_extraction.text import CountVectorizer
from collections import Counter
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--path', type=str, default='/Users/alice.naghshineh/Desktop/pickled_data/threshold_analysis/original_batches', help='Path to folder where previously pickled review data from (clean_text.py) are stored. Default is \"/Users/alice.naghshineh/Desktop/pickled_data/threshold_analysis/original_batches\"')

args = parser.parse_args()
data_path = args.path

def load_batch_data(n_batches=5, path= data_path):
    data = pd.DataFrame()
    for batch in range(n_batches):
        with open(os.path.join(path,'batch_{}.pkl'.format(batch+1)), 'rb') as f:
            pickled_data = pickle.load(f)
            data = pd.concat([data, pickled_data[0]], ignore_index=True)
        print('batch_{} loaded\n'.format(batch+1))

    return data

def _infrequent_tokens_list(tokens, n = 3):
    def dummy_fun(text):
        return text

    vectorizer = CountVectorizer(
    tokenizer = dummy_fun,
    preprocessor= dummy_fun,
    token_pattern=None)
    
    docs = vectorizer.fit_transform(tokens)
    counts = docs.sum(axis=0).A1
    features = vectorizer.get_feature_names()
    freq_distribution = Counter(dict(zip(features, counts)))
    infrequent_tokens = [token for token, count in freq_distribution.items() if count <= n]
    return infrequent_tokens

print('\nLoading in previously pickled review data...\n')
#Processed, cleaned review data were previously pickled in 5 batches. Load 5 batches into same dataframe
processed_reviews = load_batch_data(5)
print('Loading finished\n')

print('Creating list of rare tokens with threshold 30 (appear at most 30 times in corpus)...\n')
#Create list of all tokens that appear 30 or less times in the corpus
rare_tokens_30 = _infrequent_tokens_list(processed_reviews['processed_text'], 30)
print('Rare tokens list created\n')

print('Pickling rare tokens list\n')
with open('rare_tokens_threshold30.pkl', 'wb') as f:
    pickle.dump(rare_tokens_30, f)
print('Finished pickling rare tokens')