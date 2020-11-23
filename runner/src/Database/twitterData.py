import json
import tensorflow_hub as hub
import tweepy as tw
from nltk import corpus
from pymongo import MongoClient
from datetime import timedelta, datetime

db = MongoClient('db', 27017).WallStreetDB
stopwords = set(corpus.stopwords.words('english'))
access_token = "1684578204-dNsdj2saYlqGHK2FfeQjzEdrM8JCZ2YgAiR9Q9i"
access_token_secret = "klJoVpLbWYd6tgDbvUzuzNNdVdxfibdRwVoTbUTpjm4It"
consumer_key = "kP38ezBd5LroR5OjsNR2nnukO"
consumer_secret = "C2aBN1HazPuAYqTGRSCPUj4M9WkX4GZsmJxtG5hwTuXsGBJgQD"
embed = hub.load("https://tfhub.dev/google/universal-sentence-encoder/4")



def insertData():
    with open(r'tweets_11-06-2020.json', 'r') as data_file:
        data_json = json.load(data_file)
    db.Twitter_data.insert(data_json)


def tokenize():
    tweets = db.Twitter_data.find({'sentenceEncoding': {"$exists": False}})
    numDocuments = (tweets.count())
    currentDoc = 0
    for tweet in tweets:

        print(currentDoc / numDocuments, end='\r')
        embeddings = embed([
            tweet['text']])

        key = {'_id': tweet['_id']}
        db.Twitter_data.update_one(key,
                                   {'$set':
                                       {
                                           'sentenceEncoding': embeddings[0].numpy().tolist()
                                       }
                                   }, upsert=True)

        currentDoc+=1

def tokenizeEpi(text):
    sentences = text.split('.')
    embeddings = embed(
        sentences)

    sentenceList = []
    for x in embeddings:
        sentenceList.append(x.numpy().tolist())

    return sentenceList



def dot(A,B):
    return (sum(a*b for a,b in zip(A,B)))

def cosine_similarity(a,b):
    return dot(a,b) / ( (dot(a,a) **.5) * (dot(b,b) ** .5) )

def get_similarity(xList, yList):
    xSet = set(xList)
    ySet = set(yList)
    intersection = xSet.intersection(ySet)
    if (len(intersection) > 0):
        return list(intersection)
    return None


# takes user input to get latest tweets from inputted user
def searchTweets():
    count = 0
    auth = tw.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tw.API(auth, wait_on_rate_limit=True)
    print("what user's tweets do you want")
    username = input()
    print("how many tweets to download")
    download = input()

    for tweet in tw.Cursor(api.user_timeline,
                           id=username).items((int)(download)):
        count += 1
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
    tweets = db.Twitter_data.find({'isRetweet': 'f', 'similarShowsBeforeTweetEncode': {"$exists": False}})
    numDocuments = (tweets.count())
    print(numDocuments)
    currentDoc = 0
    for tweet in tweets:
        print(currentDoc / numDocuments, end='\r')

        # find episodes an hour before trumps tweets
        air_date = tweet['date']
        air_date = datetime.strptime(air_date, '%Y-%m-%d %H:%M:%S')
        lower_bound = air_date - timedelta(hours=1)
        compare_episodes = db.CleanEpisodes.find({
            'metadata.Datetime_UTC': {'$lt': air_date, '$gte': lower_bound},
            'metadata.Network': {'$eq': 'FOX Business'},
            'duplicate_of': {'$eq': []}
        })

        # find list for similar shows and common phrases
        similarities = []
        for episode in compare_episodes:
            text = ' '.join(x['transcript'] for x in episode['snippets'])
            ySet = tokenizeEpi(text)
            for y in ySet:
                similarity = cosine_similarity(tweet['sentenceEncoding'],y)
                if(similarity>.5 and episode['title'] not in similarities):
                    similarities.append(episode['title'])

        key = {'_id': tweet['_id']}
        db.Twitter_data.update_one(key,
                                   {'$set':
                                       {
                                           'similarShowsBeforeTweetEncode': similarities
                                       }
                                   }, upsert=True)
        currentDoc += 1


# find similarities in shows after tweets
def tweetLeadFox():
    tweets = db.Twitter_data.find({'isRetweet': 'f', 'similarShowsAfterTweetEncode': {"$exists": False}})
    numDocuments = (tweets.count())
    print(numDocuments)
    currentDoc = 0
    for tweet in tweets:
        print(currentDoc / numDocuments, end='\r')
        # find episodes an hour after trumps tweets
        air_date = tweet['date']
        air_date = datetime.strptime(air_date, '%Y-%m-%d %H:%M:%S')
        upper_bound = air_date + timedelta(hours=1)
        compare_episodes = db.CleanEpisodes.find({
            'metadata.Datetime_UTC': {'$lt': upper_bound, '$gte': air_date},
            'metadata.Network': {'$eq': 'FOX Business'},
            'duplicate_of': {'$eq': []}
        })

        # find list for similar shows and common phrases
        similarities = []
        for episode in compare_episodes:
            text = ' '.join(x['transcript'] for x in episode['snippets'])
            ySet = tokenizeEpi(text)
            for y in ySet:
                similarity = cosine_similarity(tweet['sentenceEncoding'],y)
                if(similarity>.5 and episode['title'] not in similarities):
                    similarities.append(episode['title'])



        key = {'_id': tweet['_id']}
        db.Twitter_data.update_one(key,
                                   {'$set':
                                       {
                                           'similarShowsAfterTweetEncode': similarities
                                       }
                                   }, upsert=True)
        currentDoc += 1


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
        elif (choice == 't'):
            tokenize()
        else:
            if (choice != 'd'):
                print('sorry that was not a valid option')
