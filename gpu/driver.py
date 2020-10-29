from Connect import connect
from NLP import getEntities
from datetime import datetime

db = connect()
start_time = datetime.now()
for i, episode in enumerate(db.CleanEpisodes.find({}).limit(10)):
    print(' ' + str(i), end='\r')
    transcript = ' '.join(x['transcript'] for x in episode['snippets'])
    entities = getEntities(transcript)
end_time = datetime.now()

diff = end_time - start_time
print(diff, i / diff.total_seconds())
