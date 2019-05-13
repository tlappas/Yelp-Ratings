import time
import nltk
import re
import pandas as pd
import pickle
import psycopg2
import unicodedata

stops = nltk.corpus.stopwords.words('english')
wnl = nltk.WordNetLemmatizer()

def for_loc(data):
    for i, text in enumerate(data.loc[:,'review_text']):
        text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf8', 'ignore')
        text = re.sub(r"[^A-Za-z0-9\s']", '', text)
        text = text.lower()
        text = re.sub(r'[\n|\r|\n\r|\r\n]', ' ', text)

        text = [word for word in text.split() if word not in stops]

        text = [wnl.lemmatize(word, pos='n') for word in text]
        text = [wnl.lemmatize(word, pos='v') for word in text]
        text = [wnl.lemmatize(word, pos='a') for word in text]
        text = [wnl.lemmatize(word, pos='r') for word in text]
        text = ' '.join(text)

        data.loc[i,'review_text'] = text

def for_at(data):
    for i, text in enumerate(data.loc[:,'review_text']):
        text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf8', 'ignore')
        text = re.sub(r"[^A-Za-z0-9\s']", '', text)
        text = text.lower()
        text = re.sub(r'[\n|\r|\n\r|\r\n]', ' ', text)

        text = [word for word in text.split() if word not in stops]

        text = [wnl.lemmatize(word, pos='n') for word in text]
        text = [wnl.lemmatize(word, pos='v') for word in text]
        text = [wnl.lemmatize(word, pos='a') for word in text]
        text = [wnl.lemmatize(word, pos='r') for word in text]
        text = ' '.join(text)

        data.at[i,'review_text'] = text

def iterrows_at(data):
    for i, row in data.iterrows():
        text = row['review_text']
        text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf8', 'ignore')
        text = re.sub(r"[^A-Za-z0-9\s']", '', text)
        text = text.lower()
        text = re.sub(r'[\n|\r|\n\r|\r\n]', ' ', text)

        text = [word for word in text.split() if word not in stops]

        text = [wnl.lemmatize(word, pos='n') for word in text]
        text = [wnl.lemmatize(word, pos='v') for word in text]
        text = [wnl.lemmatize(word, pos='a') for word in text]
        text = [wnl.lemmatize(word, pos='r') for word in text]
        text = ' '.join(text)

        data.at[i,'review_text'] = text

def apply_on_column(data):
    data.apply(lambda row: _process_review(row['review_text']), axis=1)
    return data

def _process_review(text):
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf8', 'ignore')
    text = re.sub(r"[^A-Za-z0-9\s']", '', text)
    text = text.lower()
    text = re.sub(r'[\n|\r|\n\r|\r\n]', ' ', text)

    text = [word for word in text.split() if word not in stops]

    text = [wnl.lemmatize(word, pos='n') for word in text]
    text = [wnl.lemmatize(word, pos='v') for word in text]
    text = [wnl.lemmatize(word, pos='a') for word in text]
    text = [wnl.lemmatize(word, pos='r') for word in text]
    text = ' '.join(text)

    return text

if __name__ == '__main__':

    print('\nLoading data...')
    conn = psycopg2.connect('dbname=yelp user=tlappas host=/var/run/postgresql')
    cur = conn.cursor()
    cur.execute("""
        select review.review_text
        from review, business, user_info
        where review.user_id = user_info.user_id
        and business.business_id = review.business_id
        and business.categories LIKE '%Restaurants%'
        and length(user_info.elite) != 0
        limit 100000
    """)

    cols = ['review_text']
    data = pd.DataFrame(cur.fetchall(), columns=cols)

    methods = [apply_on_column, iterrows_at, for_at, for_loc]

    for method in methods:
        data_copy = data.copy(deep=True)
        print('Processing data with {}...'.format(method.__name__))
        start = time.time()
        method(data_copy)
        end = time.time()
        dur = end - start
        # Verify that the function is working
        print('{} processed {} instances in {} minutes {} seconds.\n'.format(method.__name__, data_copy.shape[0], dur//60, dur%60))
        #print('{}\n'.format(data_copy.at[0, 'review_text']))
