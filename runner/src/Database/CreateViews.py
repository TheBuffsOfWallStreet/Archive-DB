from Database.Connect import connect

db = connect()


def createCleanEpisodes():
    '''CleanEpisodes only contains episodes that have been cleaned and have no errors.'''
    if "CleanEpisodes" in db.list_collection_names():
        print('CleanEpisodes already exists')
        return
    else:
        db.command({
            "create": "CleanEpisodes",
            "viewOn": "Episodes",
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
            'viewOn': 'Episodes',
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
    createCleanEpisodes()
    createSeriesSummary()
