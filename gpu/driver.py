from Connect import connect
from NLP import getEntities
from datetime import datetime
from Parallel import runProcesses

def doThing(episode_id):
    fork_db = connect(new=True)
    episode = fork_db.Episodes.find_one({'_id': episode_id}, {'snippets': 1})
    transcript = ' '.join(x['transcript'] for x in episode['snippets'])
    entities = getEntities(transcript)
    return None


if __name__ == '__main__':
    db = connect()
    n = 10
    start_time = datetime.now()
    ids = [episode['_id'] for episode in db.CleanEpisodes.find({}, {'_id': 1}).limit(n)]
    runProcesses(doThing, ids)
    end_time = datetime.now()

    diff = end_time - start_time
    print(diff, n / diff.total_seconds())
