import psycopg2
import nltk
import unicodedata
import pandas as pd
import pickle
import re
import os
from nltk.corpus import wordnet
import time
import sys
import argparse
from nltk.tokenize import RegexpTokenizer
wnl = nltk.WordNetLemmatizer()
nltk.download('averaged_perceptron_tagger')

parser = argparse.ArgumentParser()
parser.add_argument('-u', '--username', type=str, default='postgres', help='User to access Postgres database. Default is \"postgres\"')
parser.add_argument('-w', '--password', type=str, default='', help='Password to access database. Default is empty. Password is not needed if current user can access db.')
parser.add_argument('-d', '--dbname', type=str, default='yelp', help='Name of the Postgres database. Default is \"yelp\"')
parser.add_argument('-o', '--host', type=str, default="""/var/run/postgresql""", help="""Postgres host. Default is \"/var/run/postgresql/\"""")
parser.add_argument('-p', '--path', type=str, default='/Users/alice.naghshineh/Desktop/pickled_data/threshold_analysis/original_batches', help='Path to folder for saving pickled data. Default is \"/Users/alice.naghshineh/Desktop/pickled_data/threshold_analysis/original_batches\"')
parser.add_argument('-pn', '--progress_number', type=int, default=10000, help='Set this number N for code to update you every time N number of reviews have been processed. Default is \"10000\"')

args = parser.parse_args()
dbname = args.dbname
username = args.username
password = args.password
host = args.host.lower()
save_path = args.path
progress_number = args.progress_number

conn = psycopg2.connect('dbname={} user={} host={} password={}'.format(dbname, username, host, password))
cur = conn.cursor()

print('\nRetrieving review data from PostgreSQL db...')
cur.execute("""
    SELECT review_id, review_text as processed_text FROM review
""")

cols = ['review_id', 'processed_text']

reviews = pd.DataFrame(cur.fetchall(), columns=cols)

if reviews.shape[0] == 6685900:
    print('\nRetrieved review data from PostgreSQL db\n')
    batch_size = 6685900/5
    batch1 = reviews.loc[0:batch_size-1]
    batch2 = reviews.loc[batch_size:2*batch_size -1]
    batch3 = reviews.loc[2*batch_size:3*batch_size -1]
    batch4 = reviews.loc[3*batch_size:4*batch_size -1]
    batch5 = reviews.loc[4*batch_size:5*batch_size -1]
    if batch1.shape[0] == batch_size and batch2.shape[0] == batch_size and batch3.shape[0] == batch_size and batch4.shape[0] == batch_size and batch5.shape[0] == batch_size:
        print('Five batches of review data successfully created')

else:
	sys.exit("Error trying to retrieve review data from PostgreSQL")


def _process_review(text):
    def _create_stop_words():
        stops = nltk.corpus.stopwords.words('english')
    
        neg_stops = ['no',
         'nor',
         'not',
         'don',
         "don't",
         'ain',
         'aren',
         "aren't",
         'couldn',
         "couldn't",
         'didn',
         "didn't",
         'doesn',
         "doesn't",
         'hadn',
         "hadn't",
         'hasn',
         "hasn't",
         'haven',
         "haven't",
         'isn',
         "isn't",
         'mightn',
         "mightn't",
         'mustn',
         "mustn't",
         'needn',
         "needn't",
         'shan',
         "shan't",
         'shouldn',
         "shouldn't",
         'wasn',
         "wasn't",
         'weren',
         "weren't",
         "won'",
         "won't",
         'wouldn',
         "wouldn't",
         'but',
         "don'",
         "ain't"]

        common_nonneg_contr = ["could've",
        "he'd",
        "he'd've",
        "he'll",
        "he's",
        "how'd",
        "how'll",
        "how's",
        "i'd",
        "i'd've",
        "i'll",
        "i'm",
        "i've",
        "it'd",
        "it'd've",
        "it'll",
        "it's",
        "let's",
        "ma'am",
        "might've",
        "must've",
        "o'clock",
        "'ow's'at",
        "she'd",
        "she'd've",
        "she'll",
        "she's",
        "should've",
        "somebody'd",
        "somebody'd've",
        "somebody'll",
        "somebody's",
        "someone'd",
        "someone'd've",
        "someone'll",
        "someone's",
        "something'd",
        "something'd've",
        "something'll",
        "something's",
        "that'll",
        "that's",
        "there'd",
        "there'd've",
        "there're",
        "there's",
        "they'd",
        "they'd've",
        "they'll",
        "they're",
        "they've",
        "'twas",
        "we'd",
        "we'd've",
        "we'll",
        "we're",
        "we've",
        "what'll",
        "what're",
        "what's",
        "what've",
        "when's",
        "where'd",
        "where's",
        "where've",
        "who'd",
        "who'd've",
        "who'll",
        "who're",
        "who's",
        "who've",
        "why'll",
        "why're",
        "why's",
        "would've",
        "y'all",
        "y'all'll",
        "y'all'd've",
        "you'd",
        "you'd've",
        "you'll",
        "you're",
        "you've"]

        letters = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
          'u', 'v', 'w', 'x', 'y', 'z']
        
        ranks = ['st', 'nd', 'rd', 'th']
        
        for x in neg_stops:
            if x in stops:
                stops.remove(x)

        new_stops = stops + common_nonneg_contr + letters + ranks + [""] + ['us'] + [''] + ["'"]
        stops = list(set(new_stops))
        return stops

    def get_wordnet_pos(word):
        tag = nltk.pos_tag([word])[0][1][0].lower()
        tag_dict = {"a": wordnet.ADJ,
                    "n": wordnet.NOUN,
                    "v": wordnet.VERB,
                    "r": wordnet.ADV}
        return tag_dict.get(tag, wordnet.NOUN)

    def _clean_review(text):
        text = text.lower()
        text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf8', 'ignore')
        tokenizer = nltk.RegexpTokenizer('\w+\'?\w+')
        filtered_tokens = [(re.sub(r"[^A-Za-z\s']", '', token)) for token in tokenizer.tokenize(text)]
        stops = _create_stop_words()
        tokens = [token for token in filtered_tokens if token not in stops]
        tokens = [re.sub("'s", '', token) for token in tokens if re.sub("'s", '', token) != '']
        for i, token in enumerate(tokens):
            tokens[i] = wnl.lemmatize(token, pos= get_wordnet_pos(token))
        tokens = [token for token in tokens if token not in stops]
        return tokens
    
    return _clean_review(text)

def apply_on_column(data, progress_number):
    splits = [x for x in range(0,data.shape[0] + 1, progress_number)]
    data = data.reset_index(drop=True)
    for i in range(0, len(splits) - 1):
        m = splits[i]
        n = splits[i+1] - 1
        #I adjusted the first 'review_text' below from 'review_tokens' to cut down on memory pressure
        #In other words, a review_text column is no longer retained, as it is turned into tokens
        data.loc[m:n, 'processed_text'] = data.loc[m:n, 'processed_text'].apply(lambda x: _process_review(x))
        print("{} reviews processed...".format(n+1))
    if data.shape[0] % progress_number != 0:
        rem = data.shape[0] % progress_number
        data.loc[splits[-1]:splits[-1]+rem -1, 'processed_text'] = data.loc[splits[-1]:splits[-1]+rem -1, 'processed_text'].apply(lambda x: _process_review(x))
    return data

for i, batch in enumerate([batch1, batch2, batch3, batch4, batch5]):
    print("\nStarting to clean batch{} review data...\n".format(i+1))
    file = "batch_{}.pkl".format(i+1)
    start = time.time()
    tokenized_reviews = apply_on_column(batch, progress_number)
    end = time.time()
    dur = end - start
    # Verify that the function is working
    print('Processed {} instances in {} minutes {} seconds.\n'.format(batch.shape[0], dur//60, dur%60))


    print("Pickling batch{} data\n".format(i+1))
    with open(os.path.join(save_path, file), 'wb') as f:
        pickle.dump([tokenized_reviews], f)

    print('Checking to see if batch{} properly pickled...\n'.format(i+1))
    
    with open(os.path.join(save_path, file), 'rb') as f:
        pickled_data = pickle.load(f)

    df = pickled_data[0]

    if df.shape[0] == batch.shape[0] and df['processed_text'].isnull().values.any() == False:
        print('Batch{} was successfully pickled!'.format(i+1))

    else:
        sys.exit('Uh oh.. something went wrong during the pickling process...')
