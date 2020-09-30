import Scrape
import Episode

import os
import json
import logging

if not os.path.exists('logs'):
    os.makedirs('logs')
logging.basicConfig(filename='logs/Archive.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')


def save(text, dirname):
    '''
    Saves a given string to a given dirname.
    Creates the directory if it doesn't already exist.
    '''
    if not os.path.exists(os.path.dirname(dirname)):
        os.makedirs(os.path.dirname(dirname))
    with open(dirname, 'w') as file:
        file.write(text)


def downloadEpisodes(folder_name='Bloomberg_Transcripts', n=50000):
    '''
    Given a list of episode objects from searchAllSegments, fetches data from page and saves to disk in JSON format.
    '''
    episodes = Scrape.allEpisodes(n)
    num_failed = 0
    for i, episode in enumerate(episodes):
        try:
            print(f'Fetching Episode {i} ({i/len(episodes):.2%} done, {num_failed/len(episodes):.2%} failed)', end='\r')
            episode_data = Scrape.getEpisode(episode['identifier'])
            show = episode_data['metadata']['Title']
            datetime = episode_data['metadata']['AirDate']
            # Format string for Windows file system.
            datetime = datetime.replace(',','').replace(' ', '_').replace(':','')
            dirname = f'{folder_name}/{show}/{datetime}.json'
            save(json.dumps(episode_data), dirname)
        except Exception as e:
            num_failed += 1
            logging.error(f'Failed to fetch episode {i} with id {episode["identifier"]}')
            logging.exception(e)


def filesGenerator(folder_name='Bloomberg_Transcripts'):
    '''
    Generates paths to episodes stored locally on disk.
    A generator is more memory efficient when the database gets larger.
    '''
    for path, subdirs, files in os.walk(folder_name):
        for file in files:
            yield os.path.join(path, file)

def allEpisodes(folder_name='Bloomberg_Transcripts'):
    for path in filesGenerator(folder_name):
        yield Episode.openFile(path)