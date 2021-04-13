from pymongo import MongoClient

db = None

def connect(new=False):
    global db
    if new:
        return MongoClient('db', 27017).WallStreetDB
    if db is None:
        db = MongoClient('db', 27017).WallStreetDB
        return db
    else:
        return db