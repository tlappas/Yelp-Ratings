import pandas as pd
import pickle
import os
import time
from sklearn.feature_extraction.text import CountVectorizer
from collections import Counter
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--path', type=str, default='/Users/alice.naghshineh/Desktop/pickled_data/threshold_analysis/original_batches', help='Path to folder where previously pickled review data from (clean_text.py) are stored. Default is \"/Users/alice.naghshineh/Desktop/pickled_data/threshold_analysis/original_batches\"')
parser.add_argument('-pn', '--progress_number', type=int, default=1000, help='Set this number N for code to update you every time N number of reviews have been processed. Default is \"10000\"')

args = parser.parse_args()
data_path = args.path
progress_number = args.progress_number

with open('rare_tokens_threshold30.pkl', 'rb') as f:
    rare_tokens_30 = pickle.load(f)

if len(rare_tokens_30) == 583595:
    print('Rare tokens list retrieved')

def _remove_rare_tokens(tokens):   
    tokens_to_remove = list((set(tokens) & set(rare_tokens_30)))
    frequent_tokens = [token for token in tokens if token not in tokens_to_remove]
    return frequent_tokens

def apply_on_column(data, progress_number):
    splits = [x for x in range(0,data.shape[0] + 1, progress_number)]
    data = data.reset_index(drop=True)
    for i in range(0, len(splits) - 1):
        m = splits[i]
        n = splits[i+1] - 1
        #I adjusted the first 'review_text' below from 'review_tokens' to cut down on memory pressure
        #In other words, a review_text column is no longer retained, as it is turned into tokens
        data.loc[m:n, 'processed_text'] = data.loc[m:n, 'processed_text'].apply(lambda x: _remove_rare_tokens(x))
        print("{} reviews processed...".format(n+1))
    if data.shape[0] % progress_number != 0:
        rem = data.shape[0] % progress_number
        data.loc[splits[-1]:splits[-1]+rem -1, 'processed_text'] = data.loc[splits[-1]:splits[-1]+rem -1, 'processed_text'].apply(lambda x: _remove_rare_tokens(x))
    return data

#Process (remove rare tokens) and pickle the data in 5 batches again
for batch in ['batch_1', 'batch_2', 'batch_3', 'batch_4', 'batch_5']:
    print("\nStarting to remove rare tokens from {} review data...\n".format(batch))
    file = "{}.pkl".format(batch)
    with open(os.path.join(data_path, file), 'rb') as f:
        pickled_data = pickle.load(f)
        batch_df = pickled_data[0]
    start = time.time()
    reviews_no_rare_tokens = apply_on_column(batch_df, progress_number)
    end = time.time()
    dur = end - start
    # Verify that the function is working
    print('Processed {} instances in {} minutes {} seconds.\n'.format(batch_df.shape[0], dur//60, dur%60))


    print("Pickling {} data\n".format(batch))
    new_file = "{}_no_rare_tokens.pkl".format(batch)
    with open(os.path.join(data_path, new_file), 'wb') as f:
        pickle.dump([reviews_no_rare_tokens], f)
    print('Finished pickling {}'.format(batch))
