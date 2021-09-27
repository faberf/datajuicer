from datajuicer.database import BaseDatabase, prepare_document
import os
import threading
import tinydb
import tinydb.operations
import time
import datajuicer as dj
from functools import reduce


class TinyDB(BaseDatabase):

    def __init__(self, record_directory = "."):
        record_path = os.path.join(record_directory, "runs.json")
        self.lock_path = os.path.join(record_directory, "runs.json.lock")

        if not os.path.isdir(record_directory):
            os.makedirs(record_directory)

        if not os.path.exists(record_path):
            file = open(record_path, 'w+')
            file.close()
        
        # if not os.path.exists(lock_path):
        #     file = open(lock_path, 'w+')
        #     file.close()
        
        # self.lock = fasteners.InterProcessLock(lock_path)

        self.lock = threading.Lock()
        #portalocker.RedisLock('some_lock_channel_name')
        #portalocker.Lock(lock_path, 'rb+', timeout=TIMEOUT)
        #portalocker.Lock(os.path.join(record_directory, "runs.json.lock"), timeout=TIMEOUT)
        #threading.Lock()
        #filelock.FileLock(os.path.join(record_directory, "runs.json.lock"), TIMEOUT)
        self.db = tinydb.TinyDB(record_path)
    
    def record_run(self, run_id, func, *args, **kwargs):
        document = prepare_document(func,args,kwargs,False)
        document["run_id"] = run_id
        document["start_time"] = int(time.time()*1000)
        document["done"] = False
        
        with self.lock:
            self.db.insert(document)

    def record_done(self, run_id):
        query = tinydb.Query()
        with self.lock:
            self.db.update(tinydb.operations.set("done", True), query["run_id"] == run_id)
    
    def get_newest_run(self, func, *args, **kwargs):

        document = prepare_document(func,args,kwargs,True)

        conditions = to_conditions(document)

        unsorted = self.db.search(conditions)

        res = [k["run_id"] for k in sorted(unsorted, key= lambda k: k['start_time'])]

        if len(res) == 0:
            return None
        
        else:
            return res[-1]

    def get_all_runs(self, func=None):

        q = tinydb.Query()
        if func:
            if type(func) is str:
                func_name = func
            if callable(func):
                if not type(func) is dj.Recordable:
                    func = dj.Recordable(func)
                func_name  = func.name
            all_docs = self.db.search(q.func_name == func_name)
        else:
            all_docs = self.db.all()
        return [d["run_id"] for d in all_docs]

    def delete_runs(self, run_ids):

        q = tinydb.Query()

        with self.lock:
            for rid in run_ids:
                self.db.remove(q["run_id"] == rid)
    
    def get_raw(self):
        return self.db.all()

def to_conditions_list(query, obj):
    conditions = []

    if obj is dj.Ignore:
        return []

    if type(obj) in [list, tuple]:
        for i, item in enumerate(obj):
            conditions += to_conditions_list(query[i], item)
        return conditions

    if type(obj) is dict:
        for key, val in obj.items():
            conditions += to_conditions_list(query[key], val)
        return conditions
    
    return [query == obj]

def to_conditions(obj):
    query = tinydb.Query()
    l = to_conditions_list(query, obj)
    return reduce(lambda a,b: a & b, l)
    