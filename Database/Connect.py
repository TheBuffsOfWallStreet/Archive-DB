from pymongo import MongoClient

db = None

def connect():
    global db
    if db is None:
        db = MongoClient('localhost', 27017).WallStreetDB
        return db
    else:
        return db