from pymongo import MongoClient
from pymemcache.client.base import Client

db = None

def connect():
    global db
    domain = 'domain.com'
    username = 'USERNAME'
    password = 'PASSWORD'
    authSource = 'user_table'
    return MongoClient(domain, 27017, username=username, password=password, authSource=authSource).finlab_prod


def connect_cache():
    return Client('cache') # Docker DNS routes 'cache' -> memcache
