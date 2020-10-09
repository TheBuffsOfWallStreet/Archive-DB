from pymongo import MongoClient
from datetime import datetime, timedelta

db = MongoClient('localhost', 27017).WallStreetDB


def cleanIndexDates(episode):
    form = '%Y-%m-%dT%H:%M:%SZ'
    update = {'$set': {
        'datetime': datetime.strptime(episode['date'], form)
    }}
    db.ArchiveIndex.update_one({'_id': episode['_id']}, update)


def cleanEpisodeDuration(episode):
    duration = datetime.strptime(episode['metadata']['Duration'], '%H:%M:%S')
    duration_delta = timedelta(hours=duration.hour, minutes=duration.minute, seconds=duration.second)
    update = {'$set': {
        'Duration_s': duration_delta.seconds
    }}
    if duration_delta.seconds == 0:
        update['$addToSet'] = {'errors': 'duration_is_0'}
    db.ArchiveIndex.update_one({'_id': episode['_id']}, update)


def markEmpty(episode):
    db.ArchiveIndex.update_one({'_id': episode['_id']}, {'$addToSet': {'errors': 'transcript_is_empty'}})


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


def clean(all=False):
    # Convert index datetime (string) to datetime object.
    print('Cleaning Index Dates')
    if all:
        query = {}
    else:
        query = {'date': {'$exists': True}, 'datetime': {'$exists': False}}
    cursor = db.ArchiveIndex.find(query)
    perform(cursor, cleanIndexDates)

    # Convert Episode Duration (string) to an integer representing seconds.
    print('Cleaning Episode Duration')
    if all:
        query = {}
    else:
        query = {'metadata': {'$exists': True}, 'metadata.Duration_s': {'$exists': False}}
    cursor = db.ArchiveIndex.find(query)
    perform(cursor, cleanEpisodeDuration)

    empty_transcripts = db.ArchiveIndex.aggregate([
        {
            '$match': {
                'snippets': {
                    '$exists': True
                },
            }
        }, {
            '$addFields': {
                'length': {
                    '$reduce': {
                        'input': '$snippets.transcript',
                        'initialValue': 0,
                        'in': {
                            '$add': [
                                {
                                    '$strLenCP': '$$this'
                                }, '$$value'
                            ]
                        }
                    }
                }
            }
        }, {
            '$match': {
                'length': 0
            }
        }
    ])
    perform(empty_transcripts, markEmpty)
