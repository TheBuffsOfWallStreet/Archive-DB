from Database.CleanDuplicates import findDuplicate
from Database import connect
from Parallel import runProcesses

def cleanDuplicates():
    '''
    Searches all episodes that have not been checked for duplicates.
    Stores duplicates for each episode in database as an array.
    '''
    db = connect()
    query = {'duplicates': {'$exists': False}}
    query = {}
    cursor = db.CleanEpisodes.find(query, {
        '_id': 1,
        'metadata.Network': 1,
        'metadata.Datetime_UTC': 1
    }).sort('metadata.Datetime_UTC').limit(1000)
    runProcesses(findDuplicate, cursor, max_workers=1)

cleanDuplicates()
