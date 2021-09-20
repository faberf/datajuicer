import inspect
import copy
import time
import tinydb
import os
import pickle
import datajuicer
from functools import reduce

def record_run(record_dir, run_id, func, func_name, *args, **kwargs):

    record_path = os.path.join(record_dir, "runs.json")
    if not os.path.isdir(record_dir):
        os.makedirs(record_dir)

    if not os.path.exists(record_path):
        file = open(record_path, 'w+')
        file.close()
    db = tinydb.TinyDB(record_path)

    document = prepare_document(func,args,kwargs,False,func_name)
    document["run_id"] = run_id
    document["start_time"] = int(time.time()*1000)
    
    db.insert(document)

def prepare_document(func, args, kwargs, keep_ignores, func_name):
    boundargs = inspect.signature(func).bind(*args,**kwargs)
    boundargs.apply_defaults()

    document = {}

    for key, val in to_document(boundargs.arguments, keep_ignores).items():
        document["arg_" + key] = val

    document["func_name"] = func_name

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
    
    if type(obj) in [str, int, float, bool]:
        return obj

    else:
        return pickle.dumps(obj)

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
    

def get_newest_run(record_dir, func, func_name, *args, **kwargs):
    record_path = os.path.join(record_dir, "runs.json")

    if not os.path.exists(record_path):
        file = open(record_path, 'w+')
        file.close()
    db = tinydb.TinyDB(record_path)

    document = prepare_document(func,args,kwargs,True,func_name)

    conditions = to_conditions(document)

    res = [k["run_id"] for k in sorted(db.search(conditions), key= lambda k: k['start_time'])]

    if len(res) == 0:
        return None
    
    else:
        return res[-1]