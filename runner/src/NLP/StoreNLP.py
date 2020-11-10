from Database import connect
from NLP.NLP import getEntities, getSentiment
from Parallel import runProcesses


def storeEntities(episode_id):
    db = connect(new=True)
    episode = db.Episodes.find_one({'_id': episode_id})
    transcript = ' '.join([x['transcript'] for x in episode['snippets']])
    entities = getEntities(transcript)
    db.Episodes.update({'_id': episode_id}, {'$set': {'entities': entities}})


def runStoreEntities():
    db = connect()
    episode_ids = [x['_id'] for x in db.CleanEpisodes.find({}, {'_id': 1})]
    runProcesses(storeEntities, episode_ids, 4)


def storeSentiment(episode_id):
    db = connect(new=True)
    episode = db.Episodes.find_one({'_id': episode_id})
    print(episode_id)
    setFields = {}
    for i, snippet in enumerate(episode['snippets']):
        polarity, subjectivity = getSentiment(snippet['transcript'])
        setFields[f'snippets.{i}.polarity'] = polarity
        setFields[f'snippets.{i}.subjectivity'] = subjectivity
    db.Episodes.update({'_id': episode_id}, {'$set': setFields})


def runStoreSentiment():
    db = connect()
    episode_ids = [x['_id'] for x in db.CleanEpisodes.find({}, {'_id': 1})]
    runProcesses(storeEntities, episode_ids, 4)
