from pymongo import MongoClient
from datetime import datetime, timedelta
import pytz
import re

db = MongoClient('localhost', 27017).WallStreetDB


def markError(episode, error):
    '''Adds 'transcript_is_empty' flag to a given episode.'''
    db.ArchiveIndex.update_one({'_id': episode['_id']}, {'$addToSet': {'errors': error}})

def cleanIndexDates(episode):
    '''episode['date'] (str) -> (datetime object) episode['datetime'].'''
    form = '%Y-%m-%dT%H:%M:%SZ'
    update = {'$set': {
        'datetime': datetime.strptime(episode['date'], form)
    }}
    db.ArchiveIndex.update_one({'_id': episode['_id']}, update)


def cleanMetadatDates(episode):
    subtitle = episode['metadata']['Subtitle']
    # Match like 'CSPAN July 16, 2009 11:00pm-2:00am EDT'
    time = '(\d+):(\d+)(\w+)'
    match = re.match(f'([\w\s]+) (\w+) (\d+), (\d+) {time}-{time} (\w+)', subtitle)
    if match:
        network, month, day, year, hour, minute, ampm, hour_end, minute_end, ampm_end, timezone = match.groups()

    else:
        match = re.match(f' (\w+) (\d+), (\d+) {time}-{time} (\w+)', subtitle)
        if match:
            month, day, year, hour, minute, ampm, hour_end, minute_end, ampm_end, timezone = match.groups()
        else:
            markError(episode, 'cannot_parse_metadata_date')
            raise(Exception(f'No match for {subtitle}'))
    date = datetime.strptime(f'{year} {month} {day} {hour}:{minute} {ampm.upper()}', '%Y %B %d %I:%M %p')
    if timezone in ['EST', 'EDT']:
        timezone = 'EST5EDT'
    elif timezone in ['PST', 'PDT']:
        timezone = 'PST8PDT'
    elif timezone in ['CST', 'CDT']:
        timezone = 'CST6CDT'
    date = pytz.timezone(timezone).localize(date)
    date = date.astimezone(pytz.utc)
    db.ArchiveIndex.update_one({'_id': episode['_id']}, {'$set': {'metadata.Datetime_UTC': date}})


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

    print('Cleaning metadata Dates')
    query = {'metadata': {'$exists': True}}
    if not all:
        query['metadata.Datetime'] = {'$exists': False}
    cursor = db.ArchiveIndex.find(query)
    perform(cursor, cleanMetadatDates)

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
