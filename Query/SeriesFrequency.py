from pymongo import MongoClient
import matplotlib.pyplot as plt
from datetime import timedelta

db = MongoClient('localhost', 27017).WallStreetDB

series = db.SeriesSummary.find({}, {'num_episodes': 1}).sort('num_episodes', -1)
series = [x for x in series]

# Boundries for Buckets
res = db.CleanedIndex.aggregate([{
    '$group': {
        '_id': None,
        'min': {'$min': '$metadata.Datetime_UTC'},
        'max': {'$max': '$metadata.Datetime_UTC'}
    }
}])
mn, mx = None, None
for row in res:
    print(row)
    mn = row['min']
    mx = row['max']

# Buckets
bins = []
div = mn - timedelta(days=mn.weekday())
div.replace(hour=0, minute=0, second=0)
print(div)
while div < mx + timedelta(days=7):
    bins.append(div)
    div += timedelta(days=7)


def getSeriesBuckets(show, bins, matching='.*'):
    res = db.CleanedIndex.aggregate([{
        '$match': {
            'metadata.Title': show,
            'snippets.transcript': {
                '$regex': matching
            }
        }
    }, {
        '$bucket': {
            'groupBy': '$metadata.Datetime_UTC',
            'boundaries': bins,
            'default': 'OOB',
            'output': {
                'count': {
                    '$sum': 1
                }
            }
        }
    }])

    weeks = []
    counts = []
    for row in res:
        weeks.append(row['_id'])
        counts.append(row['count'])
    return weeks, counts


def generateReport(shows, term=None):
    sharex = None
    for i, show in enumerate(shows):
        sharex = plt.subplot(3, 4, i+1, sharex=sharex, sharey=sharex)
        print(show['_id'])
        weeks, weights = getSeriesBuckets(show['_id'], bins)
        plt.hist(weeks, weights=weights, bins=bins)

        if term:
            weeks, weights = getSeriesBuckets(show['_id'], bins, matching=f'.* {term}.*')
            plt.hist(weeks, weights=weights, bins=bins)
        plt.title(f"{show['_id']} (n={show['num_episodes']})")
        plt.xticks(rotation=60)
        if i >= 11:
            break
    plt.tight_layout()
    plt.show(block=False)
    plt.pause(0.01)


selection = None
page = 0
term = None
generateReport(series)

while selection != 'q':
    print()
    print('n) Next page')
    print('p) Prev page')
    print('s) Start')
    print(f'm) Match term (current: {term})')
    print('q) Quit')

    selection = input(' Selection:')

    if selection == 'n':
        page += 12
        if page >= len(series):
            page = 0
        plt.clf()
        generateReport(series[page:], term)

    if selection == 'p':
        page -= 12
        if page > 0:
            page = 0
        plt.clf()
        generateReport(series[page:], term)
    if selection == 's':
        page = 0
        plt.clf()
        generateReport(series[page:], term)

    if selection == 'm':
        term = input(' Term:')
        plt.clf()
        generateReport(series[page:], term)
