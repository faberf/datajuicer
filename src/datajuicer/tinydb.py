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
        self.directory = record_directory
        record_path = os.path.join(record_directory, "runs.json")

        if not os.path.isdir(record_directory):
            os.makedirs(record_directory)

        if not os.path.exists(record_path):
            file = open(record_path, 'w+')
            file.close()
        self.lock = threading.Lock()
        self.db = tinydb.TinyDB(record_path)
    
    def record_run(self, func_name, run_id, kwargs):
        table = self.db.table(func_name)
        
        document = prepare_document(func_name,kwargs,False)
        document["run_id"] = run_id
        document["start_time"] = int(time.time()*1000)
        document["done"] = False
        
        with self.lock:
            table.insert(document)

    def record_done(self, func_name, run_id):
        table = self.db.table(func_name)

        query = tinydb.Query()
        with self.lock:
            table.update(tinydb.operations.set("done", True), query["run_id"] == run_id)
    
    def get_newest_run(self, func_name, kwargs):
        table = self.db.table(func_name)

        document = prepare_document(func_name,kwargs,True)

        conditions = to_conditions(document)

        unsorted = table.search(conditions)

        res = [k["run_id"] for k in sorted(unsorted, key= lambda k: k['start_time'])]

        if len(res) == 0:
            return None
        
        else:
            return res[-1]

    def get_all_runs(self, func_name):
        all_docs = self.get_raw(func_name)
        return [d["run_id"] for d in all_docs]

    def delete_runs(self, func_name, run_ids):
        table = self.db.table(func_name)

        q = tinydb.Query()

        with self.lock:
            for rid in run_ids:
                table.remove(q["run_id"] == rid)
    
    def get_raw(self, func_name):
        return self.db.table(func_name).all()

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
    