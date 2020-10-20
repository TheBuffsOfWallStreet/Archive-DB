from pymongo import MongoClient
import matplotlib.pyplot as plt
import numpy as np
from datetime import timedelta

db = MongoClient('localhost', 27017).WallStreetDB


def getSeriesBuckets(show, bins, matching=None):
    '''
    Runs an aggregation on the database.
    Given a show, returns bins containing the number of episodes aired each week.
    '''
    match = {'metadata.Title': show}
    if matching:
        match['snippets.transcript'] = {'$regex': matching}
    res = db.CleanEpisodes.aggregate([{
        '$match': match
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


def generateReport(shows, bins, term=None):
    '''Queries and Plots frequency information for the first 12 shows in the list.'''
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


def menu():
    '''User program to navigate pages of series.'''
    series = db.SeriesSummary.find({}, {'num_episodes': 1}).sort('num_episodes', -1)
    series = [x for x in series]

    # Boundries for Buckets
    boundary_query = db.CleanEpisodes.aggregate([{
        '$group': {
            '_id': None,
            'min': {'$min': '$metadata.Datetime_UTC'},
            'max': {'$max': '$metadata.Datetime_UTC'}
        }
    }])
    mn, mx = None, None
    for row in boundary_query:  # There should only be 1 row.
        mn = row['min']
        mx = row['max']
    # Round mn to nearest week start (Sunday)
    mn -= timedelta(days=mn.weekday())
    mn.replace(hour=0, minute=0, second=0)
    bins = np.arange(mn, mx + timedelta(days=7), timedelta(days=7)).tolist()

    selection = None
    page = 0
    term = None
    generateReport(series, bins)

    while selection != 'q':
        print()
        print('n) Next page')
        print('p) Prev page')
        print('f) First page')
        print(f's) Search term (current: {term})')
        print('q) Quit')

        selection = input(' Selection:')
        generate_report = False

        if selection == 'n':  # Next Page
            page += 12
            if page >= len(series):
                page = 0
            generate_report = True

        if selection == 'p':  # Previous Page
            page -= 12
            if page > 0:
                page = 0
            generate_report = True

        if selection == 'f':  # First page
            if page != 0:
                page = 0
                generate_report = True

        if selection == 's':  # Search for term
            term = input(' Term:')
            generate_report = True

        if generate_report:
            plt.clf()
            generateReport(series[page:], bins, term)


if __name__ == '__main__':
    menu()
