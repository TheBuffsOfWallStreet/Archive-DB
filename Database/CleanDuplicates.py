from pymongo import MongoClient
from functools import lru_cache
from datetime import timedelta
from nltk import corpus
from nltk.tokenize import word_tokenize

db = MongoClient('localhost', 27017).WallStreetDB
stopwords = set(corpus.stopwords.words('english'))

def nGrams(text, n=2):
    tokens = [w for w in word_tokenize(text) if w not in stopwords]
    bag = set()
    for i in range(n, len(tokens)+1):
        bag.add(' '.join(tokens[i - n: i]))
    return bag

cacheMisses = {}  # TODO: remove debug tool.


@lru_cache(maxsize=64, typed=False)
def getBag(episode_id, n_gram=2):
    '''
    Returns a bag of n_grams for the given episodes transcript.
    Caches results to the database for quick re-access.
    '''
    # TODO: Remove debug tool
    if episode_id in cacheMisses:
        cacheMisses[episode_id] += 1
        print(f'cache miss {cacheMisses[episode_id]} {episode_id}')
    else:
        cacheMisses[episode_id] = 1

    episode = db.ArchiveIndex.find_one({'_id': episode_id}, {'snippets': 1})
    text = ' '.join(x['transcript'] for x in episode['snippets'])
    return nGrams(text, n_gram)


def jaccardSimilarity(bag1, bag2):
    return len(bag1.intersection(bag2)) / (len(bag1.union(bag2)) + 1)


def cosineSimilarity(bag1, bag2):
    print(len(bag1), len(bag2))
    mags = (len(bag1) * len(bag2))**.5
    return len(bag1.intersection(bag2)) / (mags + 1)


def findDuplicate(episode, threshold=0.5):
    '''
    Searches all episodes in the 4 days preceding the given episode.
    Returns a list of all episodes with cosine similarity greather than the given threshold.
    '''
    print(episode['_id'])
    duplicates = []
    air_date = db.ArchiveIndex.find_one({'_id': episode['_id']})['metadata']['Datetime_UTC']
    lower_bound = air_date - timedelta(days=4)

    compare_episodes = db.ArchiveIndex.find({
        'metadata.Datetime_UTC': {'$lt': air_date, '$gte': lower_bound},
        'metadata.Network': {'$eq': episode['metadata']['Network']},
    }, {
        '_id': 1
    })

    current_bag = getBag(episode['_id'])
    for compare_episode in compare_episodes:
        compare_bag = getBag(compare_episode['_id'])
        similarity = cosineSimilarity(current_bag, compare_bag)
        if similarity > threshold:
            print(similarity, compare_episode['_id'])
            duplicates.append(compare_episode['_id'])
    return duplicates


def cleanDuplicates():
    '''
    Searches all episodes that have not been checked for duplicates.
    Stores duplicates for each episode in database as an array.
    '''
    query = {'duplicates': {'$exists': False}}
    total_docs = db.ArchiveIndex.count_documents(query)
    for i, episode in enumerate(db.CleanedIndex.find(query).sort('metadata.Datetime_UTC')):
        print(f' {i}, {i/total_docs:.2%}', end='\r')
        duplicates = findDuplicate((episode))
        # db.ArchiveIndex.update_one({'_id': episode_id['_id']}, {'$set': {'duplicate_of': duplicates}})
