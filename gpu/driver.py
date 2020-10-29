from Connect import connect
from NLP import getEntities

db = connect()
for episode in db.CleanEpisodes.find({}):
    print(episode)
    transcript = [x['transcript'] for x in episode['snippets']]
    print(transcript[:500])
    entities = getEntities(transcript)
    print(entities)
    break
