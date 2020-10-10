from pymongo import MongoClient
from datetime import datetime, timedelta
from collections import Counter
import pytz
import re

db = MongoClient('localhost', 27017).WallStreetDB


def clean():
    '''
    User function to run all cleaning functions.
    if all == False, data that has already been scanned is ignored.
    '''
    updates = Counter()  # Track metrics for user
    failures = Counter()
    total_docs = db.ArchiveIndex.count_documents({})
    for i, episode in enumerate(db.ArchiveIndex.find({})):
        print(f' {i}, {i/total_docs:.1%}', end='\r')  # Progress Bar
        set_fields = {}  # Fields to update in the object
        errors = []
        if 'date' in episode:
            # Create datetime object from date string scraped from web.
            try: # Wrapped in try except in case date format changes, causing an exception.
                form = '%Y-%m-%dT%H:%M:%SZ'
                set_fields['datetime'] = datetime.strptime(episode['date'], form)
                updates['datetime'] += 1
            except:
                print('failed parsing date from date')
                failures['datetime'] += 1
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
                updates['metadata.Subtitle'] += 1
            except:
                print('Failed parsing date from subtitle.')
                failures['metadata.Subtitle'] += 1

            # Parse duration into timedelta object. Convert to seconds and store.
            try:
                duration = datetime.strptime(episode['metadata']['Duration'], '%H:%M:%S')
                duration_delta = timedelta(hours=duration.hour, minutes=duration.minute, seconds=duration.second)
                set_fields['metadata.Duration_s'] = duration_delta.seconds
                if duration_delta.seconds == 0:
                    errors.append('duration_is_0')
                updates['metadata.Duration'] += 1
            except:
                print('Failed parsing timedelta from duration')
                failures['metadata.Duration'] += 1

            # Mark episodes as empty or short according to text length.
            try:
                transcript_len = 0
                for snippet in episode['snippets']:
                    transcript_len += len(snippet['transcript'])

                if transcript_len == 0:
                    errors.append('transcript_is_empty')
                if transcript_len < 244:
                    errors.append('transcript_is_short')
                updates['transcript_len'] += 1
            except:
                print('Failed calculating segment length')
                failures['transcript_len'] += 1

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
        db.ArchiveIndex.update_one({'_id': episode['_id']}, transaction)
    print('Updates:', updates)
    print('Failures:', failures)