import multiprocessing
import threading
import datajuicer
from datajuicer.processify import processify
from datajuicer.resource_lock import ResourceLock
import datajuicer.utils as utils
import copy
import inspect
from datajuicer.logging import redirect, stop_redirect, enable_proxy
from datajuicer._global import GLOBAL, _open
import sys
import traceback
from functools import wraps, partial
import os
import pickle
import subprocess
import dill

TIMEOUT = 1.0

class Namespace:
    pass

# # @processify
def _in_new_process(kwargs, task_info, force, incognito, parent_task_name, parent_task_version, parent_run_id):
    #multiprocessing.set_start_method('spawn')
    curthread = threading.current_thread()
    #assert(type(curthread) != datajuicer.task.Run)
    task = datajuicer.Task.make(*task_info[0:-2], **task_info[-2])(dill.loads(task_info[-1]))
    datajuicer.logging.enable_proxy()
    GLOBAL.resource_lock = ResourceLock(directory=task.resource_lock.directory)
    GLOBAL.cache = task.cache
    run = Run(task, kwargs, force, incognito, False)
    pseudo_parent = Namespace()
    pseudo_parent.task = Namespace()
    if not parent_task_name is None:
        pseudo_parent.task.name = parent_task_name
        pseudo_parent.task.version = parent_task_version
        pseudo_parent.run_id = parent_run_id
        run.start(pseudo_parent)
    else:
        run.start()

    return run.get(), run.run_id

def process_func(q, *args, **kwargs):
    try:
        ret = _in_new_process(*args, **kwargs)
    except Exception:
        ex_type, ex_value, tb = sys.exc_info()
        error = ex_type, ex_value, ''.join(traceback.format_tb(tb))
        ret = None
    else:
        error = None

    q.put((ret, error))

def spawn_and_ret(*args,**kwargs):
    ctx = multiprocessing.get_context('fork')
    target = process_func
    q = ctx.Queue()
    p = ctx.Process(target=target, args=[q] + list(args), kwargs=kwargs)
    p.start()
    ret, error = q.get()
    p.join()

    if error:
        ex_type, ex_value, tb_str = error
        message = '%s (in subprocess)\n%s' % (ex_value.args, tb_str)
        raise ex_type(message)

    return ret

# def _in_new_process(q, kwargs, task, force, incognito, parent_task_name, parent_task_version, parent_run_id):
#     try:
#         #multiprocessing.set_start_method('spawn')
#         curthread = threading.current_thread()
#         assert(type(curthread) != datajuicer.task.Run)

#         datajuicer.logging.enable_proxy()
#         GLOBAL.resource_lock = task.resource_lock
#         GLOBAL.cache = task.cache
#         run = Run(task, kwargs, force, incognito, False)
#         pseudo_parent = Namespace()
#         pseudo_parent.task = Namespace()
#         if not parent_task_name is None:
#             pseudo_parent.task.name = parent_task_name
#             pseudo_parent.task.version = parent_task_version
#             pseudo_parent.run_id = parent_run_id
#             run.start(pseudo_parent)
#         else:
#             run.start()

#         ret = run.get()
#     except Exception:
#         ex_type, ex_value, tb = sys.exc_info()
#         error = ex_type, ex_value, ''.join(traceback.format_tb(tb))
#         ret = None
#     else:
#         error = None

#     q.put((ret, error))

# def launch_process(parent_uid, *other_args):
#     path = os.path.join("dj_resources", parent_uid)
#     with open(path, "wb+") as f:
#         dill.dump(other_args, f)
#     subprocess.run(["python", "datajuicer/process.py", "-path", path]).check_returncode()
#     with open(path+"out", "rb") as f:
#         ret, error = dill.load(f)
#     if error:
#         ex_type, ex_value, tb_str = error
#         message = '%s (in subprocess)\n%s' % (ex_value.args, tb_str)
#         raise ex_type(message)

#     return ret


class Run(threading.Thread):
    def __init__(self, task, kwargs, force, incognito, process):
        super().__init__()
        self._return = None
        self.task = task
        self.kwargs = kwargs
        self.run_id = None
        self.unique_id = utils.rand_id()
        self.resource_lock = task.resource_lock
        self.force = force
        self.incognito = incognito
        self.process = process
    
    def start(self, parent_thread=None) -> None:
        if not parent_thread: parent_thread = threading.currentThread()
        self.parent = parent_thread
        return super().start()

    def run(self):
        self.resource_lock.acquire()
        if self.process:
            if type(self.parent) is Run and self.parent.run_id is not None:
                parent_task_name = self.parent.task.name
                parent_task_version = self.parent.task.version
                parent_run_id = self.parent.run_id
            else:
                parent_task_name = None
                parent_task_version = None
                parent_run_id = None
            task = self.task
            task_info = [task.name, task.version, task.resource_lock, task.cache, task.dependencies, dill.dumps(task.func)]
            self._return, self.run_id = spawn_and_ret(self.kwargs, task_info, self.force, self.incognito, parent_task_name, parent_task_version, parent_run_id)
        
        else:
            self._return = self.task._run(self.kwargs, force = self.force, incognito = self.incognito)
        if type(self.parent) is Run and self.parent.run_id is not None:
            self.parent.task.cache.add_run_dependency(
                self.parent.task.name, 
                self.parent.task.version, 
                self.parent.run_id, 
                self.task.name, 
                self.task.version, 
                self.run_id)
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
    
    # def __eq__(self, other):
    #     return type(self) == type(other) and (self is other or (self.run_id == other.run_id and self.run_id is not None))
    
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
            del self.deps["keep"]
        
        if "ignore" in self.deps:
            for key in self.deps["ignore"]:
                self.deps[key] = Ignore
            del self.deps["ignore"]
        
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

def wrapper(func, name, version, resource_lock, cache, process, **dependencies):
    
    
    # t.__name__ = func.__name__ + 'dj'
    # setattr(sys.modules[__name__], t.__name__, t)
    # func.__name__ = func.__name__ + 'orig'
    # setattr(sys.modules[__name__], func.__name__, func)
    t = Task(func, name, version, resource_lock, cache, process, **dependencies)

    datajuicer.GLOBAL.task_versions[t.name] = t.version
    datajuicer.GLOBAL.task_caches[t.name] = t.cache
    return t

class Task:
    @staticmethod
    def make(name=None, version=0.0, resource_lock = None, cache=None, process=False, **dependencies):
        t = partial(wrapper, name=name, version=version, resource_lock=resource_lock, cache=cache, process=process, **dependencies)
        return t

    def __init__(self, func, name=None, version=0.0, resource_lock = None, cache=None, process=False, **dependencies):
        self.func = func
        self.lock = threading.Lock()
        self.dependencies = dependencies
        self.get_dependencies = Depend(**dependencies).modify
        self.conds = {}
        if resource_lock is None:
            resource_lock = GLOBAL.resource_lock
        if cache is None:
            cache = GLOBAL.cache
        self.cache = cache
        self.resource_lock = resource_lock
        self.version = version
        if name is None:
            name = func.__name__
        self.name = name
        self.version = version
        self.process = process


    def _run(self, kwargs, force, incognito):

        if not force:
            dependencies = self.get_dependencies(kwargs)
            for rid in self.cache.get_newest_runs(self.name, self.version, dependencies):
                def check_run_deps(cache, name, version, rid):
                    if datajuicer.GLOBAL.task_versions[name] != version:
                        return False
                    rdeps = cache.get_run_dependencies(name, version, rid)
                    for n, v, i in rdeps:
                        if not check_run_deps(datajuicer.GLOBAL.task_caches[n], n, v, i):
                            return False
                    return True
                if not check_run_deps(self.cache, self.name, self.version, rid):
                    continue
                threading.current_thread().assign_run_id(rid)
                if self.cache.is_done(self.name, self.version, rid):
                    return self.cache.get_result(self.name, self.version, rid)
                if not rid in self.conds:
                    continue
                self.resource_lock.release()
                with self.conds[rid]:
                    while not self.cache.is_done(self.name, self.version, rid):
                        self.conds[rid].wait(timeout=TIMEOUT)
                self.resource_lock.acquire()
                return self.cache.get_result(self.name, self.version, rid)
        
        rid = threading.current_thread().assign_random_run_id()
        if not incognito:
            with self.lock:
                self.conds[rid] = threading.Condition(self.lock) 
            self.cache.record_run(self.name, self.version, rid, kwargs)
            redirect(_open("log.txt", "w+"))
        result = self.func(**kwargs)
        if not incognito:
            stop_redirect()
            with self.conds[rid]:
                self.cache.record_result(self.name, self.version, rid, result)
                self.conds[rid].notify_all()
        return result

    def bind_args(self, *args, **kwargs):
        boundargs = inspect.signature(self.func).bind(*args,**kwargs)
        boundargs.apply_defaults()
        return boundargs.arguments

    def __call__(self, *args, **kwargs):
        force = False
        incognito = False
        if "force" in kwargs:
            force = kwargs["force"]
            del kwargs["force"]
        if "incognito" in kwargs:
            incognito = kwargs["incognito"]
            del kwargs["incognito"]

        kwargs = self.bind_args(*args, **kwargs)
        
        run = Run(self, kwargs, force=force, incognito=incognito, process = self.process)
        run.start()
        return run