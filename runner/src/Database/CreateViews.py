from Database.Connect import connect

db = connect()


def CreateViewIfNotExists(name, view_on, pipeline):
    if name in db.list_collection_names():
        print(f'{name} already exists')
    else:
        db.command({
            "create": name,
            "viewOn": view_on,
            "pipeline": pipeline
        })
        print(f'Created view {name} on {view_on}')


def groupBySummaryPipeline(group_by, push_id=False):
    group = {
        '_id': group_by,
        'num_episodes': {'$sum': 1},
        'total_len': {'$sum': '$transcript_str_length'},
        'total_duration': {'$sum': '$metadata.Duration_s'}
    }
    if push_id:
        group['episodes'] = {'$push': '$_id'}
    return [{'$group': group}]


def createViews():
    '''Wrapper that calls all view functions.'''
    CreateViewIfNotExists('CleanEpisodes', 'Episodes', [
        {'$match': {
            'errors': {'$exists': False},
            'transcript_str_length': {'$exists': True}
        }}
    ])
    CreateViewIfNotExists('SeriesSummary', 'Episodes', groupBySummaryPipeline('$metadata.Title'))
    CreateViewIfNotExists('NetworkSummary', 'Episodes', groupBySummaryPipeline('$metadata.Network'))
    CreateViewIfNotExists('ErrorSummary', 'Episodes', [{'$unwind': {'path': '$errors'}}] + groupBySummaryPipeline('$errors', push_id=True))
