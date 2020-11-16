import numpy as numpy
import wikipedia
from sklearn.feature_extraction.text import TfidfVectorizer

def tfidf_wiki_categories(name, threshold=0.6):
    '''
    Gets wikipedia page categories from page name and runs tfidf to
    filter out key words. Returns key words and categories
    '''
    tags = wikipedia.page(name, auto_suggest=False).categories
    vectorizer = TfidfVectorizer()
    X = vectorizer.fit_transform(tags).toarray()
    words = vectorizer.get_feature_names()
    key_words = set()
    m, n = np.shape(X)
    for i in range(m):
        for j in range(n):
            if X[i][j] >= threshold:
                key_words.add(words[j])
    return key_words, tags