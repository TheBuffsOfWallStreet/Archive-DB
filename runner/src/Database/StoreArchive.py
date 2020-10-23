# import FetchArchive as fetch
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
        data = fetch.getEpisode(episode_id)
        db.ArchiveIndex.update_one({'_id': episode_id}, {'$set': data})
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
    empty_episodes = db.ArchiveIndex.find({'metadata': {'$eq': None}})
    if n is not None:
        empty_episodes = empty_episodes.limit(n)

    with futures.ThreadPoolExecutor(max_workers=8) as executor:
        future_to_id = {executor.submit(writeEpisode, item['_id']): item['_id'] for item in empty_episodes}
        for i, future in enumerate(futures.as_completed(future_to_id)):
            id = future_to_id[future]
            succeded = future.result()
            if succeded:
                num_suceed += 1
            else:
                num_failed += 1
            print(f'{i+1} documents, {num_failed} failed ({num_failed/(i+1):.0%})', end='\r')
    print()
    print(f'downloaded {num_suceed} new documents')
