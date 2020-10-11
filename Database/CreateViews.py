from pymongo import MongoClient

db = MongoClient('localhost', 27017).WallStreetDB


def createCleanedIndex():
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
                        'metadata': {
                            '$exists': True
                        },
                        'snippets': {
                            '$exists': True
                        },
                        'datetime': {
                            '$exists': True
                        },
                        'errors': {
                            '$exists': False
                        }
                    }
                }
            ]
        })


def createSeriesSummary():
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
    createCleanedIndex()
    createSeriesSummary()
