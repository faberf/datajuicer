import datajuicer as dj
import inspect
from datajuicer.errors import RangeError, NoFramesError
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
        self.func = func
        self.n_threads = n_threads
        self.database = database
    
    def run(self, *args, **kwargs):
        _args = []
        _kwargs = {}

        frame_len = None
        for arg in list(args) + list(kwargs.values()):
            if type(arg) is dj.Frame:
                if frame_len is None:
                    frame_len = len(arg)
                elif len(arg) != frame_len:
                    raise RangeError

        if frame_len is None:
            raise NoFramesError
        
        if type(self.func) is dj.Frame:
            if len(self.func) != frame_len:
                raise RangeError
            func = self.func
        else:
            func = [self.func for _ in range(frame_len)]

        run_ids = tuple([utils.rand_id() for _ in range(frame_len)])



        for arg in args:
            if arg is RunID:
                _args.append(run_ids)
            elif not type(arg) is dj.Frame:
                _args.append([copy.copy(arg) for _ in range(frame_len)])
            else:
                _args.append(copy.copy(arg))
        
        for key, val in kwargs.items():
            if arg is RunID:
                _kwargs[key] = run_ids
            elif not type(arg) is dj.Frame:
                _kwargs[key] = [copy.copy(val) for _ in range(frame_len)]
            else:
                _kwargs[key] = copy.copy(val)

        def exec(func, __args, __kwargs, run_id):
            self.database.record_run(run_id,func, *__args, **__kwargs)
            result = func(*__args, **__kwargs)
            self.database.record_done(run_id)
            return result


        with concurrent.futures.ThreadPoolExecutor(max_workers=self.n_threads) as executor:
            futures = [executor.submit(exec, func[i], [arg[i] for arg in _args], {key:val[i] for (key, val) in _kwargs.items()}, run_ids[i]) for i in range(frame_len)]

        return dj.Frame([f.result() for f in futures])
    
    def get_runs(self, *args, **kwargs):
        _args = []
        _kwargs = {}

        frame_len = None
        for arg in list(args) + list(kwargs.values()):
            if type(arg) is dj.Frame:
                if frame_len is None:
                    frame_len = len(arg)
                elif len(arg) != frame_len:
                    raise RangeError

        if frame_len is None:
            raise NoFramesError

        for arg in args:
            if not type(arg) is dj.Frame:
                _args.append([copy.copy(arg) for _ in range(frame_len)])
            else:
                _args.append(copy.copy(arg))
        
        for key, val in kwargs.items():
            if not type(arg) is dj.Frame:
                _kwargs[key] = [copy.copy(val) for _ in range(frame_len)]
            else:
                _kwargs[key] = copy.copy(val)

        if type(self.func) is dj.Frame:
            if len(self.func) != frame_len:
                raise RangeError
            func = self.func
        else:
            func = [self.func for _ in range(frame_len)]
        

        return dj.Frame([self.database.get_newest_run(func[i], *[arg[i] for arg in _args], **{key:val[i] for (key, val) in _kwargs.items()}) for i in range(frame_len)])

class RunID:
    pass

class Ignore:
    pass


def run(func, *args, **kwargs):
    runner = Runner(func)
    return runner.run(*args, **kwargs)


