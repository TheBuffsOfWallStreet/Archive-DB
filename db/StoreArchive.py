import FetchArchive as fetch

from datetime import datetime
from pymongo import MongoClient
import os
import logging

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
    for i, item in enumerate(empty_episodes):
        try:
            episode = fetch.getEpisode(item['_id'])
            db.ArchiveIndex.update_one({'_id': item['_id']}, {'$set': episode})
            num_suceed += 1
        except Exception as e:
            logging.warning(f'Failed to parse item with id {item["_id"]}')
            logging.exception(e)
            num_failed += 1
        finally:
            print(f'{i+1} documents, {num_failed} failed ({num_failed/(i+1):.0%})', end='\r')
    print()
    print(f'downloaded {num_suceed} new documents')


if __name__ == '__main__':
    first_time = False
    if first_time:
        buildIndex()
    buildEpisodes()
