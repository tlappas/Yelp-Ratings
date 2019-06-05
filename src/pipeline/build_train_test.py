import psycopg2
import os
import pickle
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split

#Assumes default labels are yelp ratings [1,2,3,4,5]. Choose remap schema from class_combos
class_combos = {
'A': [1,2,3,4,5],
'B': [[1], [2,3,4], [5]], 
'C': [[1,2], [4,5]], 
'D': [[1,2,3], [4,5]],
'E': [[1,2,3,4], [5]],
'F': [[1,2], [3,4], [5]],
'G': [[1], [5]],
'H': [[1,2], [4,5]]
}

def remap_labels(data, class_combo = 'A'): 
    if class_combo == 'A':
        print('Using default star labels [1,2,3,4,5], 5 target classes')
        
    else:
        print('Remapping star labels to {}, {} target classes'.format(class_combos[class_combo], len(class_combos[class_combo])))
    
    def create_new_labels(star):
        if class_combo == 'A':
            return star

        if class_combo == 'B':
            labels_dict = {1:1, 2:2, 3:2, 4:2, 5:3}
            return labels_dict.get(star)

        if class_combo == 'C':
            labels_dict = {1:1, 2:1, 4:2, 5:2}
            return labels_dict.get(star, 0)

        if class_combo == 'D':
            labels_dict = {1:1, 2:1, 3:1, 4:2, 5:2}
            return labels_dict.get(star)

        if class_combo == 'E':
            labels_dict = {1:1, 2:1, 3:1, 4:1, 5:2}
            return labels_dict.get(star)

        if class_combo == 'F':
            labels_dict = {1:1, 2:1, 3:2, 4:2, 5:3}
            return labels_dict.get(star)

        if class_combo == 'G':
            labels_dict = {1:1, 5:2}
            return labels_dict.get(star, 0)

        if class_combo == 'H':
            labels_dict = {1:1, 2:1, 4:2, 5:2}
            return labels_dict.get(star, 0)
        
    data.loc[:,'stars'] = data.loc[:,'stars'].apply(lambda x: create_new_labels(x))
    df = data.loc[data['stars'] > 0]
    return df

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

def balance_classes(data):
    print('Balancing classes...')
    classes_and_counts = dict(data.stars.value_counts())
    classes = list(classes_and_counts.keys())
    min_class = min(classes_and_counts, key=classes_and_counts.get)
    min_count = classes_and_counts.get(min_class)
    undersample_classes = [c for c in classes if c != min_class]
    print('Target classes and counts in current dataset:\n{}'.format(classes_and_counts))
    print('\nUndersampling class(es) {} to match count of {} with the lowest count of {}'.format(undersample_classes,[min_class],min_count))
    df = data[data.stars == min_class]
    for c in undersample_classes:
        df_c = data[data.stars == c].sample(min_count)
        df = pd.concat([df, df_c], ignore_index=True)
        
    print('Balanced dataset created. New classes and counts:\n{}\n'.format(dict(df.stars.value_counts())))
    df = df.sample(frac=1).reset_index()
    print('Instances reduced from {} to {}'.format(data.shape[0], df.shape[0]))
    return df

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
        with open(os.path.join(path,'batch_{}_final.pkl'.format(batch+1)), 'rb') as f:
            pickled_data = pickle.load(f)
            data = pd.concat([data, pickled_data[0]], axis=1)

    return data
