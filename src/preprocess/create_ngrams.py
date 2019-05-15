from nltk.tokenize import word_tokenize
from nltk.util import ngrams

#https://stackoverflow.com/questions/17531684/n-grams-in-python-four-five-six-grams

def get_ngrams(text, n):
    n_grams = ngrams(word_tokenize(text), n)
    return [ ' '.join(grams) for grams in n_grams]

if __name__ == '__main__':
    pass