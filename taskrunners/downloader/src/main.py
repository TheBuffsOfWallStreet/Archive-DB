from Connect import connect

import requests
from bs4 import BeautifulSoup as soup
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


# Constants
BASE_URL = 'https://archive.org'

def updateArchiveIndex(network):
    '''
    Fetches the index from Archive.org.
    Contains identifiers used to construct links to episode pages.
    '''
    logger.info(f'Updating Archive Index for {network}')
    num_pages, num_episodes = 0, 0
    num_episodes_per_page = 100
    cursor = None  # Token for archive.org to get next page of results.
    url = BASE_URL + '/services/search/v1/scrape'
    while cursor != 'stop':
        num_episodes_per_page = min(num_episodes_per_page, 10000)
        payload = {
            'q': f'collection:(TV-{network})',
            'count': num_episodes_per_page,  # 10,000 is the max
            'fields': 'date,forumSubject,title,identifier',
            'sorts': 'date desc',
            'cursor': cursor,
        }
        res = requests.get(url, payload, timeout=30)
        if res.status_code != 200:
            logger.error('error: response != 200. Exiting now.')
            break
        # Success Response
        data = res.json()
        # Check if we should continue.
        if db.Episodes.find_one({'_id': data['items'][-1]['identifier']}):
            logger.info('Detected database is caught up to present.')
            cursor = 'stop'
        else:
            cursor = data.get('cursor', 'stop')
        # Handle response data
        for item in data['items']:
            query = {'_id': item['identifier']}
            set_data = query.copy()
            set_data['downloaded'] = False
            db.Episodes.update_one(query, {'$setOnInsert': set_data}, upsert=True)
        num_pages += 1
        num_episodes += len(data['items'])
        num_episodes_per_page *= 2 # exponential growth for faster initialization
    logger.info(f'Read {num_pages} pages. Checked {num_episodes} episodes.')


def webRequestEpisode(identifier):
    link = f'{BASE_URL}/details/{identifier}'
    res = requests.get(link, timeout=10)
    assert(res.status_code == 200)
    page = soup(res.text, 'html.parser')
    return page

def parseEpisodePage(page):
    parsed_data = {}

    # Fetch metadata
    meta_fields = {'Network', 'Duration', 'Source', 'Tuner', 'Scanned in', 'Tuner'}
    for metadata in page.find_all('dl', {'class': 'metadata-definition'}):
        meta_name = metadata.find('dt').text.strip()
        if meta_name in meta_fields:
            parsed_data[meta_name] = metadata.find('dd').text.strip()
        elif meta_name == 'TOPIC FREQUENCY':
            parsed_data['Topics'] = [a.text.strip() for a in metadata.find_all('a')]
    # Get Title
    title_text = page.find('div', {'class': 'tv-ttl'}).find_all(text=True)
    parsed_data['Title'] = title_text[0]
    parsed_data['Subtitle'] = title_text[1]
    # Get Date
    parsed_data['Date'] = page.find('time').text

    # Fetch snippets
    parsed_data['snippets'] = []
    for column in page.find_all('div', {'class': 'tvcol'}):
        snippet = {
            'minute': column.find('div', {'class': 'sniptitle'}).text.strip(),
            'transcript': column.find('div', {'class': 'snippet'}).text.strip(),
        }
        parsed_data['snippets'].append(snippet)

    return parsed_data


def downloadNewEpisodes():
    logger.info('Downloading new episodes')
    num_episodes, num_errors = 0, 0
    for episode in db.Episodes.find({'downloaded': False}):
        num_episodes += 1
        if num_episodes % 1000 == 0:
            logger.debug(f'downloaded {num_episodes} episodes with {num_errors} errors')
        try:
            page = webRequestEpisode(episode['_id'])
            data = parseEpisodePage(page)
            data['downloaded'] = True
            data['cleaned'] = False
            db.Episodes.update_one(episode, {'$set': data})
        except Exception as e:
            logger.exception(e)
            num_errors += 1
    logger.info(f"Done Downloading. Processed {num_episodes} episodes with {num_errors} erorrs.")

if __name__ == '__main__':
    db = connect()
    updateArchiveIndex('BLOOMBERG')
    updateArchiveIndex('FBC')
    updateArchiveIndex('CNBC')

    downloadNewEpisodes()
