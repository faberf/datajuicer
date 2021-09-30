import pickle
import datajuicer


class BaseDatabase:
    def __init__(self):
        pass
    
    def record_run(self, func, run_id, *args, **kwargs):
        pass

    def record_done(self, func, run_id):
        pass
    
    def get_newest_run(self, func, *args, **kwargs):
        pass

    def get_all_runs(self, func):
        pass

    def delete_runs(self, func, run_ids):
        pass

    def get_raw(self, func):
        pass




def prepare_document(func, args, kwargs, keep_ignores):
    if not type(func) is datajuicer.Recordable:
        func = datajuicer.Recordable(func)

    document = {}

    for key, val in to_document(func.bind_args(*args, **kwargs), keep_ignores).items():
        document["arg_" + key[4:]] = val

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

