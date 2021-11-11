import threading
import inspect
from typing import OrderedDict
import datajuicer as dj
from datajuicer import utils
from datajuicer import database
import datajuicer.errors as er
import copy
import os

TIMEOUT = 1.0

class Run(threading.Thread):
    def __init__(self, task, kwargs):
        super().__init__()
        self._return = None
        self.task = task
        self.kwargs = kwargs
        self.run_id = None
        self.resource_lock = task.resource_lock()

    def run(self):
        self.resource_lock.acquire()
        self._return = self.task._run(self.kwargs)
        self.resource_lock.free_all_resources()
        self.resource_lock.release()

    def join(self):
        self.resource_lock.release()
        super().join()
        self.resource_lock.acquire()


    def get(self):
        self.join()
        return self._return
    
    def open(self, path, mode):
        if any([x in mode for x in ["a", "w", "+"]]):
            raise Exception("Only Reading allowed")
        self.join()
        return self._open(path, mode)

    def _open(self, path, mode):
        return self.task.cache.open(self.task.name, self.task.version, self.run_id, path, mode)
    
    def assign_random_run_id(self):
        self.run_id = utils.rand_id()
        return self.run_id
    
    def assign_run_id(self, rid):
        self.run_id = rid
    
    def __eq__(self, other):
        return type(self) == type(other) and (self is other or (self.run_id == other.run_id and self.run_id is not None))
    
    def __getitem__(self, item):
        return self.kwargs[item]



class Ignore:
    pass

class Keep:
    pass

class Depend:
    def __init__(self, *keeps, **deps):
        self.deps = deps

        if "keep" in self.deps:
            for key in self.deps["keep"]:
                self.deps[key] = Keep
            for key in keeps:
                self.deps[key] = Keep
            del self.deps["key"]
        
        if "ignore" in self.deps:
            for key in self.deps["ignore"]:
                self.deps[key] = Ignore
        
    def modify(self, kwargs):
        kwargs = copy.copy(kwargs)
        default = Keep
        if Keep in self.deps.values():
            default = Ignore

        
        for key in kwargs:
            if key in self.deps:
                action = self.deps[key]
            else:
                action = default
            if action == Ignore:
                del kwargs[key]
            elif type(action) is Depend:
                kwargs[key] = action.modify(kwargs[key])

        return kwargs


        

class Task:
    @staticmethod
    def make(name=None, version=0.0, resource_lock = None, cache=None, **dependencies):
        return lambda func: Task(func, name, version, resource_lock, cache, **dependencies)

    def __init__(self, func, name=None, version=0.0, resource_lock = None, cache=None, **dependencies):
        self.func = func
        self.lock = threading.Lock()
        self.get_dependencies = Depend(**dependencies).modify
        self.conds = {}
        if resource_lock is None:
            resource_lock = dj.GLOBAL.resource_lock
        if cache is None:
            cache = dj.GLOBAL.cache
        self.cache = cache
        self.resource_lock = resource_lock
        self.version = version
        if name is None:
            name = func.__name__
        self.name = name
        self.version = version
        


    def _run(self, kwargs, force=False, incongnito=False):

        if not force:
            dependencies = self.get_dependencies(kwargs)
            rid = self.cache.get_newest_run(self.name, self.version, dependencies)
            if not rid is None:
                threading.current_thread().assign_run_id(rid)
                if self.cache.is_done(self.name, self.version, rid):
                    return self.cache.get_result(self.name, self.version, rid)
                self.resource_lock.release()
                with self.conds[rid]:
                    while not self.cache.is_done(self.name, self.version, rid):
                        self.conds[rid].wait(timeout=TIMEOUT)
                self.resource_lock.acquire()
                return self.cache.get_result(self.name, self.version, rid)
        
        rid = threading.current_thread().assign_random_run_id(rid)
        if not incongnito:
            with self.lock:
                self.conds[rid] = threading.Condition(self.lock) 
            self.cache.record_run(self.name, self.version, rid, kwargs)
        result = self.func(**kwargs)
        if not incongnito:
            with self.conds[rid]:
                self.cache.record_result(self.name, self.version, rid, result)
                self.conds[rid].notify_all()
        return result

    def bind_args(self, *args, **kwargs):
        boundargs = inspect.signature(self.func).bind(*args,**kwargs)
        boundargs.apply_defaults()
        return boundargs.arguments

    def __call__(self, *args, **kwargs):
        kwargs = self.bind_args(*args, **kwargs)
        try:
            frame = dj.Frame.make(kwargs)
            runs = [Run(self, kwargs, self.resource_lock) for kwargs in frame]
            for run in runs:
                runs.start()
            return dj.Frame(runs)
        except er.NoFramesError:
            run = Run(self, kwargs, self.resource_lock)
            run.start()
            return run
    

class Job(dj.Frame):
    def __init__(self, task, *args, **kwargs):
        pass