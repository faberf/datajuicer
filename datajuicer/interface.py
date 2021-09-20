

import copy
from datajuicer.errors import RangeError, NoFramesError
import concurrent.futures
import datajuicer.utils as utils
import datajuicer.database as database

class Frame(list):
    @staticmethod
    def new():
        return Frame([{}])

def configure(frame, configuration):
    configuration = {key:([value for _ in frame] if not type(value) is Frame else value) for (key, value) in configuration.items()}

    for val in configuration.values():
        if len(val) != len(frame):
            raise RangeError

    output = [copy.copy(datapoint) for datapoint in frame]
    for key, val_list in configuration.items():
        for (val, datapoint) in zip(val_list, output):
            datapoint[key] = val

    return Frame(output)

def vary(frame, key, values):
    if not type(key) is Frame:
        key = [key for _ in frame]
    
    if not type(values) is Frame:
        values = [values for _ in frame]

    if len(key) != len(frame) or len(values) != len(frame):
        raise RangeError
    
    for v in values:
        if not type(v) in [list, Frame]:
            raise TypeError
    
    zipped = zip(frame, key)

    output = []
    for i, (datapoint, key) in enumerate(zipped):
        for j in range(len(values[i])):
            copied = copy.copy(datapoint)
            copied[key] = values[i][j]
            output.append(copied)
    return Frame(output)

def where(frame, condition):
    if len(frame) != len(condition):
        raise RangeError
    
    if not all([type(c) is bool for c in condition]):
        raise TypeError

    return Frame([datapoint for i, datapoint in enumerate(frame) if condition[i]])

def remove_duplicates(frame):
    out = []
    for data in frame:
        unique = True
        for udata in out:
            if data == udata:
                unique = False
                continue
        if unique:
            out.append(copy.copy(data))
    
    return Frame(out)
class Runner:
    def __init__(self, func, func_name=None, n_threads=1, record_directory=None) -> None:
        if not callable(func):
            raise TypeError
        self.func = func
        if func_name:
            self.func_name = func_name
        else:
            self.func_name = func.__module__ + "." + func.__name__
        self.n_threads = n_threads
        self.record_directory = record_directory
    
    def run(self, *args, **kwargs):
        _args = []
        _kwargs = {}

        frame_len = None
        for arg in list(args) + list(kwargs.values()):
            if type(arg) is Frame:
                if frame_len is None:
                    frame_len = len(arg)
                elif len(arg) != frame_len:
                    raise RangeError

        if frame_len is None:
            raise NoFramesError

        run_ids = tuple([utils.rand_id() for _ in range(frame_len)])

        for arg in args:
            if arg is RunID:
                _args.append(run_ids)
            elif not type(arg) is Frame:
                _args.append([copy.copy(arg) for _ in range(frame_len)])
            else:
                _args.append(copy.copy(arg))
        
        for key, val in kwargs.items():
            if arg is RunID:
                _kwargs[key] = run_ids
            elif not type(arg) is Frame:
                _kwargs[key] = [copy.copy(val) for _ in range(frame_len)]
            else:
                _kwargs[key] = copy.copy(val)
        
        if self.record_directory:
            for run_id, __args, __kwargs in zip(run_ids, [[arg[i] for arg in _args] for i in range(frame_len)],  [{key:val[i] for (key, val) in _kwargs.items()} for i in range(frame_len)]):
                database.record_run(self.record_directory,run_id,self.func, self.func_name, *__args, **__kwargs)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.n_threads) as executor:
            futures = [executor.submit(self.func, *[arg[i] for arg in _args], **{key:val[i] for (key, val) in _kwargs.items()}) for i in range(frame_len)]

        return Frame([f.result() for f in futures])

class RunID:
    pass

def run(func, *args, **kwargs):
    runner = Runner(func)
    return runner.run(*args, **kwargs)

def select(frame, key):
    if not type(key) is Frame:
        key = [key for _ in frame]

    if len(key) != len(frame):
        raise RangeError
    
    return Frame([data[k] for (data,k) in zip(frame, key)] )

class Getter:
    def __init__(self, func, func_name=None, record_directory="."):
        if not callable(func):
            raise TypeError
        self.func = func
        if func_name:
            self.func_name = func_name
        else:
            self.func_name = func.__module__ + "." + func.__name__
        self.record_directory = record_directory


    def get_runs(self, *args, **kwargs):
        _args = []
        _kwargs = {}

        frame_len = None
        for arg in list(args) + list(kwargs.values()):
            if type(arg) is Frame:
                if frame_len is None:
                    frame_len = len(arg)
                elif len(arg) != frame_len:
                    raise RangeError

        if frame_len is None:
            raise NoFramesError

        for arg in args:
            if not type(arg) is Frame:
                _args.append([copy.copy(arg) for _ in range(frame_len)])
            else:
                _args.append(copy.copy(arg))
        
        for key, val in kwargs.items():
            if not type(arg) is Frame:
                _kwargs[key] = [copy.copy(val) for _ in range(frame_len)]
            else:
                _kwargs[key] = copy.copy(val)

        return Frame([database.get_newest_run(self.record_directory, self.func, self.func_name, *[arg[i] for arg in _args], **{key:val[i] for (key, val) in _kwargs.items()}) for i in range(frame_len)])

class Ignore:
    pass