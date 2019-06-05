import os
import pandas as pd
import time
import pandas
import pickle

from sklearn.metrics import f1_score
from sklearn.metrics import accuracy_score
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline

from sklearn import model_selection as ms
from sklearn.naive_bayes import GaussianNB
from sklearn.naive_bayes import MultinomialNB
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn.ensemble import BaggingClassifier
from sklearn.svm import SVC

import build_train_test

data_path = 'C:\\Users\\tom.lappas\\code\\Yelp-Ratings\\data\\processed'

classifiers = [
    RandomForestClassifier(max_depth=5), # Random Forest documentation recommends setting a default max_depth so trees don't become enormous.
    AdaBoostClassifier(),
    GaussianNB(),
    MultinomialNB(),
    BaggingClassifier(),
    KNeighborsClassifier(n_neighbors=3),
    SVC(kernel='linear')
]

#Different choices for remapping label data. Default is 'A', or [1,2,3,4,5]
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


# Import Data
#print('Loading data.')
#data = build_train_test.load_batch_data(1, data_path)
#print('Create token_count column.')
#data.loc[:,'token_count'] = data.loc[:, 'processed_text'].apply(lambda x: len(x))
#print('Convert word lists into strings.')
#data['processed_text'] = data['processed_text'].apply(' '.join)
# Get Labels
#data = build_train_test.join_labels(data, 'yelp', 'tlappas', '/var/run/postgresql/', '')
# Set Class labels
#data = build_train_test.remap_labels(data, class_combo = 'A')
#Balance classes
#data = build_train_test.balance_classes(data)
# Split into training and testing
#print('Split data into train and test sets.')
#[x_train, x_test, y_train, y_test] = build_train_test.split(data.loc[:,'processed_text'], data.loc[:,'stars'], save_data=True)

# Load train/test data with labels attached
print('Load Train/Test data.')
[x_train, y_train] = pickle.load(open(os.path.join(data_path,'train.pkl'),'rb'))
[x_test, y_test] = pickle.load(open(os.path.join(data_path,'test.pkl'),'rb'))

# Transform
print('Transform the datasets into tf-idf sparse arrays.')
tfidfer = TfidfVectorizer(ngram_range=(1,2)).fit(x_train)
x_train = tfidfer.transform(x_train)
x_test = tfidfer.transform(x_test)

# Save TF-IDF vectors so I'm not recalculating them every time...
print('Save the TF-IDFed data.\n')
pickle.dump([x_train, y_train], open(os.path.join(data_path, 'tfidfed-train.pkl'), 'wb'))
pickle.dump([x_test, y_test], open(os.path.join(data_path, 'tfidfed-test.pkl'), 'wb'))

# Load TF-IDFed train/test data with labels attached
#print('Load TF-IDFed Train/Test data.')
#[x_train, y_train] = pickle.load(open(os.path.join(data_path,'tfidfed-train.pkl'),'rb'))
#[x_test, y_test] = pickle.load(open(os.path.join(data_path,'tfidfed-test.pkl'),'rb'))

# Print data info
#print('Number of Instances: {}'.format(data.shape[0]))
print('\tTraining instances: {}'.format(x_train.shape))
print('\tTesting instances: {}\n'.format(x_test.shape))

strat_kfold = ms.StratifiedKFold(n_splits=5, shuffle=True)
print('Cross-validation: {} folds\n'.format(strat_kfold.get_n_splits()))

for estimator in classifiers:

    print('{} fitting - '.format(estimator.__class__.__name__), end='')
    # Fit model
    time_start = time.time()
    estimator.fit(x_train, y_train)
    time_stop = time.time()
    elapsed = time_stop - time_start
    print('{} minutes {} seconds'.format(elapsed // 60, elapsed % 60))

    # Predict on the training dataset
    print('{} predict training - '.format(estimator.__class__.__name__), end='')
    time_start = time.time()
    train_predictions = estimator.predict(x_train)
    time_stop = time.time()
    elapsed = time_stop - time_start
    print('{} minutes {} seconds'.format(elapsed // 60, elapsed % 60))
    print('\tTraining Accuracy Score: {}'.format(accuracy_score(y_train, train_predictions)))
    print('\tTraining F1 Score: {}\n'.format(f1_score(y_train, train_predictions, average='micro')))

    # Predict on test dataset
    print('{} predict testing - '.format(estimator.__class__.__name__), end='')
    time_start = time.time()
    test_predictions = estimator.predict(x_test)
    time_stop = time.time()
    elapsed = time_stop - time_start
    print('{} minutes {} seconds'.format(elapsed // 60, elapsed % 60))
    print('\tTesting Accuracy Score: {}'.format(accuracy_score(y_test, test_predictions)))
    print('\tTesting F1 Score: {}'.format(f1_score(y_test, test_predictions, average='micro')))
    print('\n')
