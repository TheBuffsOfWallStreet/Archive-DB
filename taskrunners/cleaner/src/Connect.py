from pymongo import MongoClient

def connect():
    domain = 'domain.com'
    username = 'user'
    password = 'password'
    authSource = 'auth'
    return MongoClient(domain, 27017, username=username, password=password, authSource=authSource).finlab_beta


from pymemcache.client.base import Client

def connect_cache():
    return Client('cache') # Docker DNS routes 'cache' -> memcache
