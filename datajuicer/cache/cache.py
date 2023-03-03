import os
from datajuicer.cache.query import Ignore
from datajuicer.ipc.lock import Lock


class DontSort:
    pass

#TODO what are the type signatures of these methods?

class Cache:

    def __init__(self, directory):
        self.directory = directory
    
    def get_lock(self, name):
        return Lock(os.path.join(self.directory, "locks"), name)
    
    def load_from_disk(self):
        raise Exception
    
    def get_hash(self):
        return hash(tuple([doc["id"] for doc in self.search(Ignore())]))

    def search(self, query, sort_key=DontSort):
        raise Exception

    def delete(self, query, last_hash = None):
        raise Exception

    def insert(self, fields, last_hash=None):
        raise Exception

    def update(self, query, fields, last_hash=None):
        raise Exception

