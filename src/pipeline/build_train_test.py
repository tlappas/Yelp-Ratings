import psycopg2
import os
import pickle
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split

#Assumes labels are yelp ratings [1,2,3,4,5]. Remap must be a list of length 5.
def remap_labels(labels, new_labels):
    return [new_labels[label-1] for label in labels]

def split(all_data, labels, prop_test = 0.3, make_arrays=True, save_data=False):
    [x_train, x_test, y_train, y_test] = train_test_split(all_data, labels, test_size=prop_test, stratify=labels)

    if make_arrays:
        if 'DataFrame' in str(type(all_data)) or 'Series' in str(type(all_data)):
            x_train = x_train.to_numpy()
            x_test = x_test.to_numpy()
        if 'DataFrame' in str(type(labels)) or 'Series' in str(type(labels)):
            y_train = y_train.to_numpy()
            y_test = y_test.to_numpy()

    if save_data:
        with open('train.pkl', 'wb') as f:
            pickle.dump([x_train, y_train], f)
        with open('test.pkl', 'wb') as f:
            pickle.dump([x_test, y_test], f)

    return [x_train, x_test, y_train, y_test]

def join_labels(data, dbname, username, host, password):
    conn = psycopg2.connect('dbname={} user={} host={} password={}'.format(dbname, username, host, password))
    cur = conn.cursor()
    cur.execute("""
        SELECT review_id, stars FROM review;
    """)
    labels = pd.DataFrame(cur.fetchall(), columns=['review_id','stars'])
    # convert stars to int8 to reduce memory usage
    labels.loc[:,'stars'] = pd.to_numeric(labels.loc[:,'stars'], 'coerce').fillna(0).astype(np.int8)
    # Inner join with the processed text data
    return pd.merge(data, labels, how='inner', on='review_id', sort=False)

# (Sequentially) loads n_blocks pickle files of Alice's processed data
def load_batch_data(n_batches, path=''):
    data = pd.DataFrame()
    for batch in range(n_batches):
        with open(os.path.join(path,'batch_{}.pkl'.format(batch+1)), 'rb') as f:
            pickled_data = pickle.load(f)
            data = pd.concat([data, pickled_data[0]], axis=1)

    return data
