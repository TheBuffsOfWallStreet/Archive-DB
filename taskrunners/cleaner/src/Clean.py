from Connect import connect

from datetime import datetime, timedelta
from collections import Counter
import pytz
import re

import logging
import sys

# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)

db = connect()


def cleanEpisode(episode):
    '''Detects errors in episode and writes changes to database'''
    set_fields = {}  # Fields to update in the object
    tags = []
    if 'Subtitle' in episode:
        # Parse datetime from Subtitle attrubite. Includes hour, endtime, and timezone.
        # Stores datetime in UTC time.
        try:
            subtitle = episode['Subtitle']
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
            set_fields['Datetime_UTC'] = date.astimezone(pytz.utc).replace(tzinfo=None)
            set_fields['Datetime_Eastern'] = date.astimezone(pytz.timezone('EST5EDT')).replace(tzinfo=None)
        except Exception as e:
            logger.exception(e)
            tags.append('error_parsing_datetime_from_metadata.Subtitle')
    else:
        tags.append('missing Subtitle field')

    if 'Duration' in episode:
        # Parse duration into timedelta object. Convert to seconds and store.
        try:
            duration = datetime.strptime(episode['Duration'], '%H:%M:%S')
            duration_delta = timedelta(hours=duration.hour, minutes=duration.minute, seconds=duration.second)
            set_fields['Duration_s'] = duration_delta.seconds
            if duration_delta.seconds == 0:
                tags.append('duration_is_0')
        except Exception as e:
            logger.exception(e)
            tags.append('error_parsing_duration_from_metadata.Duration')
    else:
        tags.append('missing Duration field')
    if 'snippets' in episode:
        # Mark episodes as empty or short according to text length.
        transcript_len = 0
        for snippet in episode['snippets']:
            transcript_len += len(snippet['transcript'])
        set_fields['transcript_str_length'] = transcript_len
        if transcript_len == 0:
            tags.append('transcript_is_empty')
        if transcript_len < 244:
            tags.append('transcript_is_short')
    else:
        tags.append('missing_snippets')

    # Remove empty transcripts.
    transaction = {
        '$pull': {'snippets': {'transcript': ''}}
    }
    set_fields['cleaned'] = True
    if tags:
        set_fields['tags'] = tags
        set_fields['is_clean'] = False
    else:
        transaction['$unset'] = {'tags': 1}
        set_fields['is_clean'] = True
        set_fields['checked_duplicate'] = False
    if set_fields:  # $set cannot be empty.
        transaction['$set'] = set_fields

    db.Episodes.update_one({'_id': episode['_id']}, transaction)


def clean():
    '''
    User function to run all cleaning functions.
    if all == False, data that has already been scanned is ignored.
    '''
    query = {
        'cleaned': False,
    }
    # query = {
    #     '$or': [
    #         {'cleaned': False},
    #         {'Datetime_UTC': {
    #             '$gt': # TODO: today - 30 days
    #         }}
    #     ]
    #     ,
    # }
    # query = {
    #     'downloaded': True,
    # }
    for i, episode in enumerate(db.Episodes.find(query)):
        if i % 1000 == 0:
            logger.info(f'cleaned {i} episodes')
        try:
            cleanEpisode(episode)
        except Exception as e:
            logger.exception(e)
    logger.info('done cleaning episodes')
