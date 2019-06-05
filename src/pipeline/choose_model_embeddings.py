# -*- coding: utf-8 -*-
"""
Created on Tue Jun  4 09:11:17 2019

@author: Havlin_M
"""
import os
import time
import pandas
import pickle

from sklearn.metrics import f1_score
from sklearn.metrics import accuracy_score
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline
import numpy
from sklearn import model_selection as ms
from sklearn.naive_bayes import GaussianNB
from sklearn.naive_bayes import MultinomialNB
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn.ensemble import BaggingClassifier
from sklearn.svm import SVC
from sklearn.base import TransformerMixin
from sklearn.linear_model import LogisticRegression
from sklearn.discriminant_analysis import QuadraticDiscriminantAnalysis
from sklearn.gaussian_process import GaussianProcessClassifier
from sklearn.gaussian_process.kernels import RBF
#from yellowbrick.classifier import ConfusionMatrix
from sklearn.pipeline import Pipeline
import build_train_test

#Set path below leading to finalized, pickled batches of review data (i.e., files with name batch_#_final.pkl)
data_path = ''
#Set path below leading to Glove txt file for embedding
glove_path = ''


class MeanEmbeddingTransformer(TransformerMixin):
    
    def __init__(self):
        self._vocab, self._E = self._load_words()
        
    
    def _load_words(self):
        E = {}
        vocab = []

        with open(os.path.join(glove_path, 'glove.6B.200d.txt'), encoding="utf8") as file:
            for i, line in enumerate(file):
                l = line.split(' ')
                if l[0].isalpha():
                    v = [float(i) for i in l[1:]]
                    E[l[0]] = numpy.array(v)
                    vocab.append(l[0])
        return numpy.array(vocab), E            

    
    def _get_word(self, v):
        for i, emb in enumerate(self._E):
            if numpy.array_equal(emb, v):
                return self._vocab[i]
        return None
    #in _doc_mean Ithink maybe we should get rid of w.lower.strip
    def _doc_mean(self, doc):
        return numpy.mean(numpy.array([self._E[w.lower().strip()] for w in doc if w.lower().strip() in self._E]), axis=0)
    
    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
        return numpy.array([self._doc_mean(doc) for doc in X])
    
    def fit_transform(self, X, y=None):
        return self.fit(X).transform(X)


classifiers = [
    RandomForestClassifier(max_depth=5), # Random Forest documentation recommends setting a default max_depth so trees don't become enormous.
    AdaBoostClassifier(),
    QuadraticDiscriminantAnalysis(),
    LogisticRegression(),
    GaussianNB(),
    MultinomialNB(),
    BaggingClassifier(),
    KNeighborsClassifier(n_neighbors=3),
    GaussianProcessClassifier()
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
print('Loading data.')
data = build_train_test.load_batch_data(1, data_path)
print('Create token_count column.')
data.loc[:,'token_count'] = data.loc[:, 'processed_text'].apply(lambda x: len(x))
print('Convert word lists into strings.')
data['processed_text'] = data['processed_text'].apply(' '.join)
# Get Labels
data = build_train_test.join_labels(data, 'yelp', 'tlappas', '/var/run/postgresql/', '')
# Set Class labels
data = build_train_test.remap_labels(data, class_combo = 'A')
#Balance classes - uncomment line below to balance classes
#data = build_train_test.balance_classes(data)
# Split into training and testing
print('Split data into train and test sets.')
#Added param make_arrays=False to split below, as numpy array input for embedding transformer errored
[x_train, x_test, y_train, y_test] = build_train_test.split(data.loc[:,'processed_text'], data.loc[:,'stars'], save_data=True, make_arrays=False)

# Load train/test data with labels attached
#print('Load Train/Test data.')
#[x_train, y_train] = pickle.load(open(os.path.join(data_path,'train.pkl'),'rb'))
#[x_test, y_test] = pickle.load(open(os.path.join(data_path,'test.pkl'),'rb'))



# Transform
print('Create word embeddings.')


#models = []
#for form in (QuadraticDiscriminantAnalysis, KNeighborsClassifier, LogisticRegression, GaussianNB,  GaussianProcessClassifier):
#    models.append(create_pipeline(x_train,form(), MeanEmbeddingTransformer(), 1))


#tfidfer = TfidfVectorizer(ngram_range=(1,2)).fit(x_train)
#x_train = tfidfer.transform(x_train)
#x_test = tfidfer.transform(x_test)


embedder = MeanEmbeddingTransformer().fit(x_train)
x_train = embedder.transform(x_train)
x_test = embedder.transform(x_test)




# Save TF-IDF vectors so I'm not recalculating them every time...
#print('Save the TF-IDFed data.\n')
#pickle.dump([x_train, y_train], open(os.path.join(data_path, 'meanembedding-train.pkl'), 'wb'))
#pickle.dump([x_test, y_test], open(os.path.join(data_path, 'meanembedding-test.pkl'), 'wb'))

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
    print('\tTraining Accuracy Score: {}'.format(accuracy_score(train_predictions, y_train)))
    print('\tTraining F1 Score: {}\n'.format(f1_score(train_predictions, y_train, average='micro')))

    # Predict on test dataset
    print('{} predict testing - '.format(estimator.__class__.__name__), end='')
    time_start = time.time()
    test_predictions = estimator.predict(x_test)
    time_stop = time.time()
    elapsed = time_stop - time_start
    print('{} minutes {} seconds'.format(elapsed // 60, elapsed % 60))
    print('\tTesting Accuracy Score: {}'.format(accuracy_score(test_predictions, y_test)))
    print('\tTesting F1 Score: {}'.format(f1_score(test_predictions, y_test, average='micro')))
    print('\n')
