from pymongo import MongoClient

db = None

def connect(new=False):
    global db
    if new:
        return MongoClient('127.0.0.1', 27017).WallStreetDB
    if db is None:
        db = MongoClient('127.0.0.1', 27017).WallStreetDB
        return db
    else:
        return db
