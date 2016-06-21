from pymongo import MongoClient


def mongo_client(*args, **kwargs):
    return MongoClient(*args, **kwargs)
