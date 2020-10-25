from Parallel import runProcesses

from Database.Connect import connect

from datetime import datetime, timedelta
from collections import Counter
import pytz
import re

db = connect()


def cleanEpisode(episode_id):
    '''Detects errors in episode and writes changes to database'''
    fork_db = connect(new=True)
    episode = fork_db.ArchiveIndex.find_one({'_id': episode_id})
    set_fields = {}  # Fields to update in the object
    errors = []
    if 'date' in episode:
        # Create datetime object from date string scraped from web.
        form = '%Y-%m-%dT%H:%M:%SZ'
        set_fields['datetime'] = datetime.strptime(episode['date'], form)
    else:
        errors.append('no_date')
    if 'metadata' in episode:
        # Parse datetime from Subtitle attrubite. Includes hour, endtime, and timezone.
        # Stores datetime in UTC time.
        try:
            subtitle = episode['metadata']['Subtitle']
            # Match 'CSPAN July 16, 2009 11:00pm-2:00am EDT'
            time = '(\d+):(\d+)(\w+)'
            match = re.match(f'.* (\w+) (\d+), (\d+) {time}-{time} (\w+)', subtitle)
            month, day, year, hour, minute, ampm, hour_end, minute_end, ampm_end, timezone = match.groups()
            date = datetime.strptime(f'{year} {month} {day} {hour}:{minute} {ampm.upper()}', '%Y %B %d %I:%M %p')
            if timezone in ['EST', 'EDT']:
                timezone = 'EST5EDT'
            elif timezone in ['PST', 'PDT']:
                timezone = 'PST8PDT'
            elif timezone in ['CST', 'CDT']:
                timezone = 'CST6CDT'
            date = pytz.timezone(timezone).localize(date)
            set_fields['metadata.Datetime_UTC'] = date.astimezone(pytz.utc)
        except:
            errors.append('failed_parsing_datetime_from_metadata.Subtitle')

        try:
            # Parse duration into timedelta object. Convert to seconds and store.
            duration = datetime.strptime(episode['metadata']['Duration'], '%H:%M:%S')
            duration_delta = timedelta(hours=duration.hour, minutes=duration.minute, seconds=duration.second)
            set_fields['metadata.Duration_s'] = duration_delta.seconds
            if duration_delta.seconds == 0:
                errors.append('duration_is_0')
        except:
            errors.append('failed_parsing_duration_from_metadata.Duration')

    if 'snippets' in episode:
        # Mark episodes as empty or short according to text length.
        transcript_len = 0
        for snippet in episode['snippets']:
            transcript_len += len(snippet['transcript'])
        set_fields['transcript_str_length'] = transcript_len
        if transcript_len == 0:
            errors.append('transcript_is_empty')
        if transcript_len < 244:
            errors.append('transcript_is_short')

    # Remove empty transcripts.
    transaction = {
        '$pull': {'snippets': {'transcript': ''}}
    }
    if errors:
        set_fields['errors'] = errors
    else:
        transaction['$unset'] = {'errors': 1}
    if set_fields:  # $set cannot be empty.
        transaction['$set'] = set_fields

    fork_db.ArchiveIndex.update_one({'_id': episode['_id']}, transaction)


def clean(all=True):
    '''
    User function to run all cleaning functions.
    if all == False, data that has already been scanned is ignored.
    '''
    query = {}
    if not all:
        query = {
            'transcript_str_length': {'$exists': False},
            'metadata': {'$exists': True},
        }
    episode_ids = [episode['_id'] for episode in db.ArchiveIndex.find(query, {'_id': 1})]
    runProcesses(cleanEpisode, episode_ids, 3)
