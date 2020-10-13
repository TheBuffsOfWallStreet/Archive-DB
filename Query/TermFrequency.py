from pymongo import MongoClient

db = MongoClient('localhost', 27017).WallStreetDB


def getTermFrequency(term):
    '''
    Given a term to search for
    prints rate of usage per number of characters
    and the percentage of episodes that the term appears in.
    '''
    show_length = db.SeriesSummary.find({'num_episodes': {'$gt': 15}})
    shows = {}
    for show in show_length:
        shows[show['_id']] = show

    term_counts = db['ArchiveIndex'].aggregate([{
        '$match': {
            'snippets': {'$exists': True},
            'errors': {'$exists': False}
        }
    }, {
        '$addFields': {
            'snippets': {
                '$filter': {
                    'input': '$snippets',
                    'cond': {
                        '$regexMatch': {
                            'input': '$$this.transcript',
                            'regex': term
                        }
                    }
                }
            }
        }
    }, {
        '$match': {'snippets': {'$not': {'$size': 0}}}
    }, {
        '$group': {
            '_id': '$metadata.Title',
            'term_episodes': {'$sum': 1},
            'num_snippets': {'$sum': {'$size': '$snippets'}}
        }
    }])

    for row in term_counts:
        if row['_id'] in shows:
            show = shows[row['_id']]
            show.update(row)
            show['frequency'] = 1000000 * show['num_snippets'] / show['total_len']
            show['episode_percent'] = show['term_episodes'] / show['num_episodes']

    for show, info in sorted(shows.items(), key=lambda x: x[1].get('frequency', 0), reverse=True):
        print(f"  {show[:40]:<40}  {info.get('frequency', 0):6.1f} per million charachters, appears in {info.get('episode_percent', 0):4.0%} of episodes")


if __name__ == '__main__':
    import sys

    if(len(sys.argv) < 2):
        print(' No Input')
        print(' Usage: python3 TermFrequency.py [Term1] [Term2] [...]')
    else:
        for term in sys.argv[1:]:
            print('searching for:', term)
            getTermFrequency(f'.* {term}.*')
            print()
