import pickle
import datajuicer


class BaseDatabase:
    def __init__(self):
        pass
    
    def record_run(self, func_name, run_id, kwargs):
        pass

    def record_done(self, func_name, run_id):
        pass
    
    def get_newest_run(self, func_name, kwargs):
        pass

    def get_all_runs(self, func_name):
        pass

    def delete_runs(self, func_name, run_ids):
        pass

    def get_raw(self, func_name):
        pass




def prepare_document(func_name, kwargs, keep_ignores):
    document = {}

    for key, val in to_document(kwargs, keep_ignores).items():
        document["arg_" + key[4:]] = val

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
            out[func(key,func)] = func(val, func)
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
    if type(obj) is str:
        return "str_" + obj
    return f"hash_{hash(pickle.dumps(obj))}"

