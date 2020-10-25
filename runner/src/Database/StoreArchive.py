from Parallel import runProcesses

from Database import FetchArchive as fetch
from Database.Connect import connect

import os
import logging
from concurrent import futures

if not os.path.exists('logs'):
    os.makedirs('logs')
logging.basicConfig(filename='logs/StoreArchive.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

db = connect()


def buildIndex(network):
    '''Gets, cleans and stores data from archiveIndexGenerator().'''
    for item in fetch.archiveIndexGenerator(network):
        # TODO: Add lastModified field.
        item['_id'] = item['identifier']
        key = {'_id': item['_id']}
        # Upsert inserts (instead of update) if the item does not exist.
        db.ArchiveIndex.update_one(key, {'$set': item}, upsert=True)


def writeEpisode(episode_id):
    '''Use fetch module to get episode data, then write to database.'''
    try:
        fork_db = connect(new=True)
        data = fetch.getEpisode(episode_id)
        fork_db.ArchiveIndex.update_one({'_id': episode_id}, {'$set': data})
        return True
    except Exception as e:
        logging.exception(e)
        return False


def buildEpisodes(n=None):
    '''
    Searches local index for empty episodes (documents with no metadata).
    Scrapes data and updates document.
    '''
    num_failed = 0
    num_suceed = 0
    empty_episodes = db.ArchiveIndex.find({'metadata': {'$eq': None}}, {'_id': 1})
    if n is not None:
        empty_episodes = empty_episodes.limit(n)

    runProcesses(writeEpisode, [item['_id'] for item in empty_episodes], max_workers=4)
