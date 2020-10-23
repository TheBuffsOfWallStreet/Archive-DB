from Database.Connect import connect

db = connect()


def createCleanedIndex():
    '''CleanedIndex only contains episodes that have been cleaned and have no errors.'''
    if "CleanedIndex" in db.list_collection_names():
        print('CleanedIndex already exists')
        return
    else:
        db.command({
            "create": "CleanedIndex",
            "viewOn": "ArchiveIndex",
            "pipeline": [
                {
                    '$match': {
                        'errors': {
                            '$exists': False
                        },
                        'transcript_str_length': {
                            '$exists': True
                        }
                    }
                }
            ]
        })


def createSeriesSummary():
    '''SeriesSummary stores aggregate information about series like the number of episodes.'''
    if "SeriesSummary" in db.list_collection_names():
        print('SeriesSummary already exists')
        return
    else:
        db.command({
            'create': 'SeriesSummary',
            'viewOn': 'CleanedIndex',
            'pipeline': [
                {
                    '$group': {
                        '_id': '$metadata.Title',
                        'num_episodes': {
                            '$sum': 1
                        },
                        'total_len': {
                            '$sum': '$transcript_str_length'
                        },
                        'total_duration': {
                            '$sum': '$metadata.Duration_s'
                        }
                    }
                }
            ]
        })


def createViews():
    '''Wrapper that calls all view functions.'''
    createCleanedIndex()
    createSeriesSummary()
