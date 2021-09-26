import inspect
import copy
import time
import tinydb
import os
import pickle
import datajuicer
import tinydb.operations
from functools import reduce

def prepare_db(record_directory):
    record_path = os.path.join(record_directory, "runs.json")
    if not os.path.isdir(record_directory):
        os.makedirs(record_directory)

    if not os.path.exists(record_path):
        file = open(record_path, 'w+')
        file.close()
    return tinydb.TinyDB(record_path)

def record_run(record_directory, run_id, func, *args, **kwargs):

    db = prepare_db(record_directory)

    document = prepare_document(func,args,kwargs,False)
    document["run_id"] = run_id
    document["start_time"] = int(time.time()*1000)
    document["done"] = False
    
    db.insert(document)

def record_done(record_dir, run_id):
    db = prepare_db(record_dir)
    query = tinydb.Query()
    db.update(tinydb.operations.set("done", True), query["run_id"] == run_id)

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
    

def get_newest_run(record_dir, func, *args, **kwargs):
    record_path = os.path.join(record_dir, "runs.json")
    if not os.path.isdir(record_dir):
        os.makedirs(record_dir)

    if not os.path.exists(record_path):
        file = open(record_path, 'w+')
        file.close()
    db = tinydb.TinyDB(record_path)

    document = prepare_document(func,args,kwargs,True)

    conditions = to_conditions(document)

    res = [k["run_id"] for k in sorted(db.search(conditions), key= lambda k: k['start_time'])]

    if len(res) == 0:
        return None
    
    else:
        return res[-1]

def get_all_runs(record_directory=".", func=None):
    db = prepare_db(record_directory)

    q = tinydb.Query()
    if func:
        if type(func) is str:
            func_name = func
        if callable(func):
            if not type(func) is datajuicer.Recordable:
                func = datajuicer.Recordable(func)
            func_name  = func.name
        all_docs = db.search(q.func_name == func_name)
    else:
        all_docs = db.all()
    return [d["run_id"] for d in all_docs]

def delete_runs(record_directory,run_ids):
    db = prepare_db(record_directory)

    q = tinydb.Query()

    for rid in run_ids:
        print(f"removing {rid}")
        db.remove(q["run_id"] == rid)