from pymongo import MongoClient
from datetime import datetime, timedelta

db = MongoClient('localhost', 27017).WallStreetDB


def cleanIndexDates(episode):
    '''episode['date'] (str) -> (datetime object) episode['datetime'].'''
    form = '%Y-%m-%dT%H:%M:%SZ'
    update = {'$set': {
        'datetime': datetime.strptime(episode['date'], form)
    }}
    db.ArchiveIndex.update_one({'_id': episode['_id']}, update)


def cleanEpisodeDuration(episode):
    '''
    episode['metadata']['Duration'] (str) ->
        episode['metadata']['Duration_s']
    If duration is 0, add 'duration_is_0' flag to errors set.
    '''
    duration = datetime.strptime(episode['metadata']['Duration'], '%H:%M:%S')
    duration_delta = timedelta(hours=duration.hour, minutes=duration.minute, seconds=duration.second)
    update = {'$set': {
        'metadata.Duration_s': duration_delta.seconds
    }}
    if duration_delta.seconds == 0:
        update['$addToSet'] = {'errors': 'duration_is_0'}
    db.ArchiveIndex.update_one({'_id': episode['_id']}, update)


def markEmpty(episode):
    '''Adds 'transcript_is_empty' flag to a given episode.'''
    db.ArchiveIndex.update_one({'_id': episode['_id']}, {'$addToSet': {'errors': 'transcript_is_empty'}})


def perform(cursor, action):
    '''Wraps cleaning funcitons in a try except loop in case of divide by 0 error or date format changes.'''
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
    '''
    User function to run all cleaning functions.
    if all == False, data that has already been scanned is ignored.
    '''
    print('Cleaning Index Dates')
    query = {'date': {'$exists': True}}
    if not all:
        query['datetime'] = {'$exists': False}  # ignore rows that already have datetime fields.
    perform(db.ArchiveIndex.find(query), cleanIndexDates)

    # Convert Episode Duration (string) to an integer representing seconds.
    print('Cleaning Episode Duration')
    query = {'metadata': {'$exists': True}}
    if not all:
        query['metadata.Duration_s'] = {'$exists': False}
    perform(db.ArchiveIndex.find(query), cleanEpisodeDuration)

    # This query was generated with MongoDB Compass.
    # View intermediate results of the pipeline by pasting into Compass.
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
