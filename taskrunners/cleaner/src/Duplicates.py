from Connect import connect, connect_cache

import json
from datetime import timedelta
from nltk import corpus
from nltk.tokenize import word_tokenize
import logging
import sys

# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)

db = connect()
cache = connect_cache()
stopwords = set(corpus.stopwords.words('english'))
cache_misses = {}


def jaccardSimilarity(bag1, bag2):
    return len(bag1.intersection(bag2)) / (len(bag1.union(bag2)) + 1)


def cosineSimilarity(bag1, bag2):
    mags = (len(bag1) * len(bag2))**.5
    return len(bag1.intersection(bag2)) / (mags + 1)


def nGrams(text, n):
    '''Returns a set of n_gram tokens for the given string of text.'''
    tokens = [w for w in word_tokenize(text) if w not in stopwords]
    bag = set()
    for i in range(n, len(tokens) + 1):
        bag.add(' '.join(tokens[i - n: i]))
    return bag


def getBag(episode_id, n_gram):
    '''
    Returns a bag of n_grams for the given episodes transcript.
    Caches results to the database for quick re-access.
    '''
    bag = cache.get(episode_id+str(n_gram))
    if bag:
        logging.debug('cache hit')
        bag = set(json.loads(bag))
    else:
        if episode_id in cache_misses:
            cache_misses[episode_id] += 1
            logger.debug(f'Cache miss: {cache_misses[episode_id]}, {episode_id}')
        else:
            cache_misses[episode_id] = 1
        episode = db.Episodes.find_one({'_id': episode_id}, {'snippets': 1})
        text = ' '.join(x['transcript'] for x in episode['snippets'])
        bag = nGrams(text, n_gram)
        cache.set(episode_id+str(n_gram), json.dumps(list(bag)), 180)
    return bag


def findDuplicate(episode, threshold=0.3, n_gram=2, n_days=7):
    '''
    Searches all episodes in the 4 days preceding the given episode.
    Returns a list of all episodes with cosine similarity greather than the given threshold.
    '''
    duplicates = []
    episode_date = episode['Datetime_UTC']
    lower_bound = episode_date - timedelta(days=n_days)

    compare_episodes = db.CleanEpisodes.find({
        'Datetime_UTC': {'$lt': episode_date, '$gte': lower_bound},
        'Network': {'$eq': episode['Network']},
    }, {'_id': 1})

    current_bag = getBag(episode['_id'], n_gram)
    for compare_episode in compare_episodes:
        compare_bag = getBag(compare_episode['_id'], n_gram)
        similarity = cosineSimilarity(current_bag, compare_bag)
        if similarity > threshold:
            duplicates.append({
                'episode_id': compare_episode['_id'],
                'score': similarity
            })
    db.Episodes.update_one({
        '_id': episode['_id']
    }, {
        '$set': {
            f'duplicate_of_{n_gram}_gram': duplicates,
            'checked_duplicate': True,
            'is_duplicate': (len(duplicates) > 0)
        }
    })
    return len(duplicates)


def cleanDuplicates(threshold=0.15, n_gram=5, n_days=7):
    '''
    Searches all episodes that have not been checked for duplicates.
    Stores duplicates for each episode in database as an array.
    '''
    query = {'checked_duplicate': False}
    cursor = db.CleanEpisodes.find(query, {
        '_id': 1,
        'Network': 1,
        'Datetime_UTC': 1
    }).sort('Datetime_UTC')
    n_duplicates = 0
    for i, episode in enumerate(cursor):
        n_matches = findDuplicate(episode, threshold, n_gram, n_days)
        if n_matches > 0:
            n_duplicates += 1
        if i % 1000 == 0:
            logger.info(f'checked {i} episodes. Found {n_duplicates} duplicates')
    logger.info('Done cleaning for duplicates')
