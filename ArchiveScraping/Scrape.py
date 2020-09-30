import requests
from bs4 import BeautifulSoup as soup
import json

BASE_URL = 'https://archive.org'


def parseEpisode(page):
    '''Given a beautiful soup document, returns parsed information about the episode as a dictionary.'''
    episode = {'snippets': [], 'metadata': {}}
    # Fetch snippets
    for column in page.find_all('div', {'class': 'tvcol'}):
        time = column.find('div', {'class': 'sniptitle'}).text.strip()
        caption = column.find('div', {'class': 'snippet'}).text.strip()
        episode['snippets'].append([time, caption])
    # Fetch metadata
    meta_fields = {'Network', 'Duration', 'Source', 'Tuner', 'Scanned in', 'Tuner'}
    for metadata in page.find_all('dl', {'class': 'metadata-definition'}):
        meta_name = metadata.find('dt').text.strip()
        if meta_name in meta_fields:
            episode['metadata'][meta_name] = metadata.find('dd').text.strip()
        elif meta_name == 'TOPIC FREQUENCY':
            episode['metadata']['Topics'] = [a.text.strip() for a in metadata.find_all('a')]
    # Get Title
    title_text = page.find('div', {'class': 'tv-ttl'}).find_all(text=True)
    episode['metadata']['Title'] = title_text[0]
    episode['metadata']['AirDate'] = title_text[1]
    # Get Date
    episode['metadata']['Date'] = page.find('time').text
    return episode

def getEpisode(identifier):
    '''
    Scrapes date from a given link to a episode on archive.org.
    Saves minute-by-minute snippets and episode metadata.
    Returns a dictionary.
    '''
    link = BASE_URL + '/details/' + identifier
    res = requests.get(link)
    assert(res.status_code == 200)
    return parseEpisode(soup(res.text))


def searchEpisodes(cursor=None, count=100):
    '''
    Makes a request to the Archive scraping API.
    Returns loaded JSON data as a dictionary.
    '''
    payload = {
        'q': 'TV-BLOOMBERG',
        'count': count,
        'fields': 'date,forumSubject,title,identifier',
        'sorts': 'date',
        'cursor': cursor,
    }
    res = requests.get(BASE_URL + '/services/search/v1/scrape', payload)
    assert(res.status_code == 200)
    return json.loads(res.text)


def allEpisodes(max_len=100000):
    '''
    Uses searchSegments until max_len episodes are found or no cursor object is returned (meaning we have fetched all results).
    '''
    cursor = None
    all_episodes = []
    i = 0
    while len(all_episodes) < max_len:
        print(f'Fetching episodes on page {i}, found {len(all_episodes)} episodes already')
        i += 1
        data = searchEpisodes(cursor, count=min(max_len, 10000))
        all_episodes += data['items']
        cursor = data.get('cursor')
        if not cursor:
            break
    count = len(all_episodes)
    print(f'Found {count} episodes, {data["total"] - count} remaining')
    return all_episodes
