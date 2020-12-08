import argparse
import os
import json
import re
import random
import inspect

def _output(func):
    def decorated(session_id, save_dir, *args, **kwargs):
        file = os.path.join(save_dir, str(session_id) + ".json")
        exists = os.path.isfile(file)
        directory = os.path.dirname(file)
        if not os.path.isdir(directory):
            os.makedirs(directory)
        if exists:
            data = json.loads(open(file).read())
        else:
            data = {}
        data = func(data, *args, **kwargs)
        with open(file,'w+') as f:
            out = re.sub('(?<!")NaN(?!")','"NaN"', json.dumps(data))
            f.write(out)
    return decorated

def load_session(session_id, save_dir):
    file = os.path.join(save_dir, str(session_id) + ".json")
    if not os.path.isfile(file):
        raise Exception
    with open(file, 'r'):
        data = json.loads(open(file).read())
    


@_output
def _log(data, key, value):
    if key in data:
        data[key] += [value]
    else:
        data[key]=[value]
    return data

@_output
def _done(data):
    data["dj_progress"] = "done"
    return data

@_output
def _update_log(data, new):
    data.update(new)
    return data


def djlab_loader(sid, table, cache_dir):
    make_dir(cache_dir)
    path = os.path.join(cache_dir, f"{table}_{sid}.pickle")
    with open(path, mode="rb") as file:
        return pickle.load(file)
    
def djlab_checker(sid, table, cache_dir):
    try:
        easy_loader(sid, table, cache_dir)
    except Exception as er:
        print(er)
        return False
    return True

class CustomFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.MetavarTypeHelpFormatter):
    pass

class ArgumentParser(argparse.ArgumentParser):

    def __init__(self, *args,**kwargs):
        self.dj_parents = []
        super().__init__(*args, **kwargs, formatter_class=CustomFormatter)
    
    def add_argument(self, *args, **kwargs, cache_ignore=False):
        boundargs = inspect.signature(super().add_argument).bind(*args,**kwargs)
        boundargs.apply_defaults()
        
        help = boundargs.arguments["help"]
        if cache_ignore:
            help += ()

    def parse_args_dj(self, *args, **kwargs):
        helper = argparse.ArgumentParser()
        helper.add_argument("-session_id", type=int, default = 0)
        helper.add_argument("-dj_save_dir", type=str, default = "djlab/logs")
        flags, left = self.parse_known_args()
        other = helper.parse_args(left)

        if other.session_id==0:
            other.session_id = random.randint(1000000000, 9999999999)

        
        
        flags.log = lambda key, value: _log(other.session_id, other.dj_save_dir, key, value)
        flags.done = lambda : _done(other.session_id, other.dj_save_dir)
        flags.update_log = lambda data: _update_log(other.session_id, other.dj_save_dir, data)
        flags.load_session = lambda session_id: load_session(session_id, other.save_dir)
        return flags