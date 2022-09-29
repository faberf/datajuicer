


import random
from threading import Lock
from datajuicer.hash import get_hash
from datajuicer.lock import FileData
import dill as pickle


class LookUp:

    def __init__(self, scratch):
        self.scratch = scratch
        self.table = {}
    
    def load(self):
        with self._get_lock():
            self._load()

    def _load(self):

        fd = self._get_file_data()
        if not fd.get() == FileData.NoData:
            self.table.update(pickle.loads(fd.get()))
    
    def _get_lock(self):
        return self.scratch.get_lock("lookup_lock")
    
    def _get_file_data(self):
        fd = self.scratch.get_file_data("lookup_data", binary=True)
        
        return fd
    

    def remember(self, request, run):
        self.table[get_hash(request)] = run.run_id
        if random.random()<0.1:
            self.save()
    
    def lookup(self, request):
        h = get_hash(request)
        return self.table.get(h,None)
    
    def save(self):
        with self._get_lock():
            self._load()
            fd = self._get_file_data()
            fd.set(pickle.dumps(self.table))



