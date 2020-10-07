# import FetchArchive as fetch
from db import FetchArchive as fetch

from datetime import datetime
from pymongo import MongoClient
import os
import logging
from concurrent import futures

if not os.path.exists('logs'):
    os.makedirs('logs')
logging.basicConfig(filename='logs/StoreArchive.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

db = MongoClient('localhost', 27017).WallStreetDB


def buildIndex():
    '''Gets, cleans and stores data from archiveIndexGenerator().'''
    form = '%Y-%m-%dT%H:%M:%SZ'
    for item in fetch.archiveIndexGenerator():
        # TODO: Add lastModified field.
        item['_id'] = item['identifier']
        key = {'_id': item['_id']}
        if 'date' in item:  # Clean index date. (str -> datetime)
            item['datetime'] = datetime.strptime(item['date'], form)
        # Upsert inserts (instead of update) if the item does not exist.
        db.ArchiveIndex.update_one(key, {'$set': item}, upsert=True)


def buildEpisodes():
    '''
    Searches local index for empty episodes (documents with no metadata).
    Scrapes data and updates document.
    '''
    num_failed = 0
    num_suceed = 0
    empty_episodes = db.ArchiveIndex.find({'metadata': {'$eq': None}})

    with futures.ThreadPoolExecutor(max_workers=8) as executor:
        future_to_id = {executor.submit(fetch.getEpisode, item['_id']): item['_id'] for item in empty_episodes}
        for i, future in enumerate(futures.as_completed(future_to_id)):
            try:
                id = future_to_id[future]
                data = future.result()
                db.ArchiveIndex.update_one({'_id': id}, {'$set': data})
                num_suceed += 1
            except Exception as e:
                logging.warning(f'Item with id {id} threw exception.')
                logging.exception(e)
                num_failed += 1
            finally:
                print(f'{i+1} documents, {num_failed} failed ({num_failed/(i+1):.0%})', end='\r')
    print()
    print(f'downloaded {num_suceed} new documents')


if __name__ == '__main__':
    build_index = input("Do you want to build the index? (y/n) ")
    if build_index in 'Yy':
        buildIndex()
    buildEpisodes()
