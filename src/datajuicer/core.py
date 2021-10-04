import datajuicer as dj
import inspect
import datajuicer.errors as er
import datajuicer.frames as frames
import concurrent.futures
import datajuicer.utils as utils
import datajuicer.database as database
import copy

class Recordable:
    def __init__(self, func, name=None):
        if name:
            self.name = name
        else:
            self.name = func.__module__ + "." + func.__name__
        self.func = func
    
    def __call__(self, *args, **kwds):
        return self.func.__call__(*args, **kwds)
    
    def bind_args(self, *args, **kwargs):
        boundargs = inspect.signature(self.func).bind(*args,**kwargs)
        boundargs.apply_defaults()
        return boundargs.arguments

def recordable(name=None):
    return lambda func: Recordable(func, name)

class Runner:
    def __init__(self, func, n_threads=1, database = database.BaseDatabase()) -> None:
        if type(func) is dj.Frame:
            for f in func:
                if not callable(f):
                    raise TypeError
        elif not callable(func):
            raise TypeError
        
        if not type(func) is Recordable:
            self.func = Recordable(func)
        else:
            self.func = func
        
        self.n_threads = n_threads
        self.database = database
    
    def _prepare_kwargs(self, *args, **kwargs):
        kwargs = self.func.bind_args(*args, **kwargs)

        frame_len = None
        for arg in kwargs.values():
            if type(arg) is dj.Frame:
                if frame_len is None:
                    frame_len = len(arg)
                elif len(arg) != frame_len:
                    raise er.RangeError

        if frame_len is None:
            raise er.NoFramesError
        
        return kwargs, frame_len
    
    def run(self, *args, **kwargs):
        kwargs, frame_len = self._prepare_kwargs(*args, **kwargs)
        
        run_ids = frames.Frame([utils.rand_id() for _ in range(frame_len)])

        kwargs = _replace(kwargs, RunID, run_ids)
        
        kwargs_frame = frames.prepare_obj(kwargs, frame_len)



        def exec(_kwargs, run_id):
            self.database.record_run(self.func.name,run_id, _kwargs)
            try:
                result = self.func(**_kwargs)
            except TypeError:
                result = self.func(*_kwargs.values())
            self.database.record_done(self.func.name, run_id)
            return result


        with concurrent.futures.ThreadPoolExecutor(max_workers=self.n_threads) as executor:
            futures = [executor.submit(exec, kwargs_frame[i], run_ids[i]) for i in range(frame_len)]

        return dj.Frame([f.result() for f in futures])
    
    def get_runs(self, *args, **kwargs):
        kwargs, frame_len = self._prepare_kwargs(*args, **kwargs)
        
        kwargs_frame = frames.prepare_obj(kwargs, frame_len)

        if type(self.func) is dj.Frame:
            if len(self.func) != frame_len:
                raise er.RangeError
            func_names = self.func
        else:
            func_names = [self.func for _ in range(frame_len)]
        
        for i, item in enumerate(func_names):
            if callable(item):
                if type(item) is Recordable:
                    func_names[i] = item.name
                else:
                    func_names[i] = Recordable(item).name
        

        return dj.Frame([self.database.get_newest_run(func_names[i], kwargs_frame[i]) for i in range(frame_len)])


def _replace(obj, val1, val2):
    if type(obj) in [list, frames.Frame]:
        out = []
        for i, val in enumerate(obj):
            out.append(_replace(val, val1, val2))
        if type(obj) is frames.Frame:
            out = frames.Frame(out)
        return out
    
    if type(obj) is dict:
        out = {}
        for key, val in obj.items():
            out[key] = _replace(val, val1, val2)
        return out
    
    if obj==val1:
        return val2
    
    return obj
            

class RunID:
    pass

class Ignore:
    pass


def run(func, *args, **kwargs):
    runner = Runner(func)
    return runner.run(*args, **kwargs)


