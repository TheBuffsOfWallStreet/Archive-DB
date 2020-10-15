import requests
from pymongo import MongoClient
from bs4 import BeautifulSoup as soup

db = MongoClient('localhost', 27017).WallStreetDB
BASE_URL = 'https://archive.org'
def archiveIndexGenerator():
    '''
    Fetches the index from Archive.org.
    Contains identifiers used to construct links to episode pages.
    '''
    i, n = 0, 0  # Store page_num, segment_count
    cursor = None  # Token for archive.org to get next page of results.
    url = BASE_URL + '/services/search/v1/scrape'
    while i == 0 or cursor is not None:
        print(f'Fetching segments on page {i}, found {n} segments already')
        # TODO: Update payload['q'] to fetch other shows.
        payload = {
            'q': 'collection:(TV-BLOOMBERG)',
            'count': 10000,  # 10,000 is the max
            'fields': 'date,forumSubject,title,identifier',
            'sorts': 'date',
            'cursor': cursor,
        }
        res = requests.get(url, payload)
        assert(res.status_code == 200)
        data = res.json()
        for item in data['items']:
            yield item
        cursor = data.get('cursor')
        i += 1
        n += len(data['items'])
    print(f'Found {n} segments')
def getEpisode(identifier):
    '''
    Makes web requests to archive.org. Scrapes episode data.
    Returns a dictionary like {metadata: {}, snippers: {}}
    '''
    link = BASE_URL + '/details/' + identifier
    res = requests.get(link, timeout=2)
    assert(res.status_code == 200)
    page = soup(res.text)
    segment = {'snippets': [], 'metadata': {}}
    # Fetch snippets
    for column in page.find_all('div', {'class': 'tvcol'}):
        snippet = {
            'minute': column.find('div', {'class': 'sniptitle'}).text.strip(),
            'transcript': column.find('div', {'class': 'snippet'}).text.strip(),
        }
        segment['snippets'].append(snippet)
    # Fetch metadata
    meta_fields = {'Network', 'Duration', 'Source', 'Tuner', 'Scanned in', 'Tuner'}
    for metadata in page.find_all('dl', {'class': 'metadata-definition'}):
        meta_name = metadata.find('dt').text.strip()
        if meta_name in meta_fields:
            segment['metadata'][meta_name] = metadata.find('dd').text.strip()
        elif meta_name == 'TOPIC FREQUENCY':
            segment['metadata']['Topics'] = [a.text.strip() for a in metadata.find_all('a')]
    # Get Title
    title_text = page.find('div', {'class': 'tv-ttl'}).find_all(text=True)
    segment['metadata']['Title'] = title_text[0]
    segment['metadata']['Subtitle'] = title_text[1]
    # Get Date
    segment['metadata']['Date'] = page.find('time').text
    return segment
