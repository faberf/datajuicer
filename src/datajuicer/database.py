import time
import tinydb
import os
import pickle
import datajuicer
import tinydb.operations
from functools import reduce
import sqlite3
import json

import datajuicer.filelock as filelock
import portalocker
import threading
import fasteners

TIMEOUT = 1

class Database:
    def __init__(self):
        pass
    
    def record_run(self, run_id, func, *args, **kwargs):
        pass

    def record_done(self, run_id):
        pass
    
    def get_newest_run(self, func, *args, **kwargs):
        pass

    def get_all_runs(self, func=None):
        pass

    def delete_runs(self, record_directory,run_ids):
        pass

class TinyDB(Database):

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
                if not type(func) is datajuicer.Recordable:
                    func = datajuicer.Recordable(func)
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
        




def prepare_document(func, args, kwargs, keep_ignores):
    if not type(func) is datajuicer.Recordable:
        func = datajuicer.Recordable(func)

    document = {}

    for key, val in to_document(func.bind_args(*args, **kwargs), keep_ignores).items():
        document["arg_" + key] = val

    document["func_name"] = func.name

    return document

def _recurse_or_ignore(obj, func):
        if obj is datajuicer.Ignore:
            return obj
        else:
            return _serialize(obj, func)

def to_document(obj, keep_ignores = False):
    if keep_ignores:
        return _recurse_or_ignore(obj, _recurse_or_ignore)

    return _serialize(obj, _serialize)

def _serialize(obj, func):

    if type(obj) is dict:
        out = {}
        for key, val in obj.items():
            out[key] = func(val, func)
        return out
    
    if type(obj) is list:
        out = []
        for item in obj:
            out.append(func(item, func))
        return out
    
    if type(obj) is tuple:
        out = []
        for item in obj:
            out.append(func(item, func))
        return tuple(out)
    
    if type(obj) in [int, float, bool]:
        return obj
    elif type(obj) is str:
        return "str_" + obj
    else:
        return f"hash_{hash(pickle.dumps(obj))}"

def to_conditions_list(query, obj):
    conditions = []

    if obj is datajuicer.Ignore:
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
    

