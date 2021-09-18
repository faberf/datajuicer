

import copy
from datajuicer.errors import RangeError
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
        key = [copy.copy(key) for _ in frame]
    
    if not type(values) is Frame:
        values = [copy.copy(values) for _ in frame]

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
    raise NotImplementedError

def remove_duplicates(frame):
    raise NotImplementedError

class Runner:
    def __init__(self, n_threads=1, incognito=False) -> None:
        self.n_threads = n_threads
        self.incognito = incognito
    
    def run(frame, func, *args, **kwargs):
        raise NotImplementedError

def run(frame, func, *args, **kwargs):
    runner = Runner()
    return runner.run(frame, func, *args, **kwargs)

def run_parallel(n_threads, frame, func, *args, **kwargs):
    raise NotImplementedError

def select(key):
    raise NotImplementedError

def get_runs(frame, func, *args, **kwargs):
    raise NotImplementedError