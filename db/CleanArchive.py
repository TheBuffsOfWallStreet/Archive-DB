from pymongo import MongoClient
from datetime import datetime, timedelta

db = MongoClient('localhost', 27017).WallStreetDB


def cleanIndexDates(episode):
    form = '%Y-%m-%dT%H:%M:%SZ'
    episode['datetime'] = datetime.strptime(episode['date'], form)
    db.ArchiveIndex.update_one({'_id': episode['_id']}, {'$set': episode})


def cleanEpisodeDuration(episode):
    metadata = episode['metadata']
    duration = datetime.strptime(metadata['Duration'], '%H:%M:%S')
    duration_delta = timedelta(hours=duration.hour, minutes=duration.minute, seconds=duration.second)
    metadata['Duration_s'] = duration_delta.seconds
    db.ArchiveIndex.update_one({'_id': episode['_id']}, {'$set': episode})


def perform(cursor, action):
    updated, failed = 0, 0
    for episode in cursor:
        try:
            action(episode)
            updated += 1
        except Exception as e:
            print(e)
            failed += 1
    print(f' {action.__name__} Updated {updated} documents. {failed} failed.')


def clean():
    # Convert index datetime (string) to datetime object.
    print('Cleaning Index Dates')
    cursor = db.ArchiveIndex.find({'date': {'$exists': True}, 'datetime': {'$exists': False}})
    perform(cursor, cleanIndexDates)

    # Convert Episode Duration (string) to an integer representing seconds.
    print('Cleaning Episode Duration')
    cursor = db.ArchiveIndex.find({'metadata': {'$exists': True}, 'metadata.Duration_s': {'$exists': False}})
    perform(cursor, cleanEpisodeDuration)
