from pymongo import MongoClient
from datetime import timedelta
from nltk import corpus
from nltk.tokenize import word_tokenize

db = MongoClient('localhost', 27017).WallStreetDB
stopwords = set(corpus.stopwords.words('english'))


def getBag(episode, n_gram=2):
    '''
    Returns a bag of n_grams for the given episodes transcript.
    Caches results to the database for quick re-access.
    '''
    cached = db.WordsCache.find_one({'_id': episode['_id'], 'n_gram': n_gram})
    if cached:
        return set(cached['bag'])
    else:
        text = ' '.join(x['transcript'] for x in episode['snippets'])
        tokens = [w for w in word_tokenize(text) if w not in stopwords]
        bag = set()
        for i in range(n_gram, len(tokens)):
            bag.add(' '.join(tokens[i - n_gram: i]))
        db.WordsCache.insert({
            '_id': episode['_id'],
            'bag': list(bag),
            'n_gram': n_gram,
        })
        return bag


def findDuplicate(episode, threshold=0.5):
    '''
    Searches all episodes in the 4 days preceding the given episode.
    Returns a list of all episodes with cosine similarity greather than the given threshold.
    '''
    duplicates = []
    air_date = db.ArchiveIndex.find_one({'_id': episode['_id']})['metadata']['Datetime_UTC']
    lower_bound = air_date - timedelta(days=4)

    compare_episodes = db.ArchiveIndex.find({
        'metadata.Datetime_UTC': {'$lt': air_date, '$gte': lower_bound},
        'metadata.Network': {'$eq': episode['metadata']['Network']},
    })

    current_bag = getBag(episode)
    for compare_episode in compare_episodes:
        compare_bag = getBag(compare_episode)
        cos_similarity = len(current_bag.intersection(compare_bag)) / (len(current_bag.union(compare_bag)) + 1)
        if cos_similarity > threshold:
            duplicates.append(compare_episode['_id'])
    return duplicates


def cleanDuplicates():
    '''
    Searches all episodes that have not been checked for duplicates.
    Stores duplicates for each episode in database as an array.
    '''
    query = {'duplicates': {'$exists': False}}
    total_docs = db.ArchiveIndex.count_documents(query)
    for i, episode in enumerate(db.CleanedIndex.find(query)):
        print(f' {i}, {i/total_docs:.2%}', end='\r')
        duplicates = findDuplicate((episode))
        db.ArchiveIndex.update_one({'_id': episode['_id']}, {'$set': {'duplicate_of': duplicates}})
