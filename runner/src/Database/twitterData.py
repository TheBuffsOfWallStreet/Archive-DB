import tweepy as tw
from nltk import corpus
from nltk.tokenize import word_tokenize
from pymongo import MongoClient
from datetime import timedelta

db = MongoClient('db', 27017).WallStreetDB
stopwords = set(corpus.stopwords.words('english'))
access_token = ""
access_token_secret = ""
consumer_key = ""
consumer_secret = ""


def tokenize(string):
    n = 2
    tokens = [w for w in word_tokenize(string) if w not in stopwords]
    bag = set()
    for i in range(n, len(tokens) + 1):
        bag.add(' '.join(tokens[i - n: i]))
    return list(bag)


def get_similarity(xList, yList):
    xSet = set(xList)
    ySet = set(yList)
    intersection = xSet.intersection(ySet)
    if (len(intersection) > 0):
        return list(intersection)
    return None


# takes user input to get latest tweets from inputted user
def searchTweets():
    auth = tw.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tw.API(auth, wait_on_rate_limit=True)
    search_words = "#MAGA"
    date_since = "2020-11-01"
    print("what user's tweets do you want")
    username = input()
    print("how many tweets to download")
    download = input()
    tweets = tw.Cursor(api.user_timeline,
                       id=username,
                       since=date_since).items((int)(download))
    for tweet in tweets:
        if (tweet.text[0:2] != 'RT'):
            key = {'_id': tweet.id_str}
            bow = tokenize(tweet.text)
            db.Twitter_data.update_one(key,
                                       {'$set':
                                           {
                                               'text': tweet.text,
                                               'created_at': tweet.created_at,
                                               'bagOfWords': bow,
                                               'username': username
                                           }
                                       }, upsert=True)


# find similarities in shows before tweets
def tweetLagFox():
    print('Type the username of tweets you want to compare')
    username = input()
    tweets = db.Twitter_data.find({'username': username,
                                  'similarShowsBeforeTweet':{'$exists': False}
    })
    for tweet in tweets:

        # find episodes an hour before trumps tweets
        air_date = tweet['created_at']
        lower_bound = air_date - timedelta(hours=1)
        compare_episodes = db.CleanEpisodes.find({
            'metadata.Datetime_UTC': {'$lt': air_date, '$gte': lower_bound},
            'metadata.Network': {'$eq': 'FOX Business'},
            'duplicate_of': { '$eq': [] }
        })

        # find list for similar shows and common phrases
        similarities = []
        for episode in compare_episodes:
            text = ' '.join(x['transcript'] for x in episode['snippets'])
            ySet = tokenize(text)
            common = get_similarity(tweet['bagOfWords'], ySet)
            if (common != None):
                similarities.append((episode['title'], common))

        key = {'_id': tweet['_id']}
        db.Twitter_data.update_one(key,
                                   {'$set':
                                       {
                                            'similarShowsBeforeTweet': similarities
                                       }
                                   }, upsert=True)


# find similarities in shows after tweets
def tweetLeadFox():
    print('Type the username of tweets you want to compare')
    username = input()
    tweets = db.Twitter_data.find({'username': username,
                                   'similarShowsAfterTweet':{'$exists': False}
                                   })
    for tweet in tweets:

        # find episodes an hour after trumps tweets
        air_date = tweet['created_at']
        upper_bound = air_date + timedelta(hours=1)
        compare_episodes = db.CleanEpisodes.find({
            'metadata.Datetime_UTC': {'$lt': upper_bound, '$gte': air_date},
            'metadata.Network': {'$eq': 'FOX Business'},
            'duplicate_of': { '$eq': [] }
        })

        # find list for similar shows and common phrases
        similarities = []
        for episode in compare_episodes:
            text = ' '.join(x['transcript'] for x in episode['snippets'])
            ySet = tokenize(text)
            common = get_similarity(tweet['bagOfWords'], ySet)
            if (common != None):
                similarities.append((episode['title'], common))

        key = {'_id': tweet['_id']}
        db.Twitter_data.update_one(key,
                                   {'$set':
                                       {
                                           'similarShowsAfterTweet': similarities
                                       }
                                   }, upsert=True)


if __name__ == '__main__':
    choice = ''
    while (choice != 'd'):
        print('What function do you want to run\n'
              'a) download tweets\n'
              'b) tweet lead fox\n'
              'c) tweet lag fox\n'
              'd) quit')

        choice = input()
        if (choice == 'a'):
            searchTweets()
        elif (choice == 'b'):
            tweetLeadFox()
        elif (choice == 'c'):
            tweetLagFox()
        else:
            if (choice != 'd'):
                print('sorry that was not a valid option')
