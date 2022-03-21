
if __name__ == "__main__":
    import sys
    import os
    sys.path.append(os.getcwd())

import threading
import inspect
import copy
import time
import datajuicer.utils as utils
import datajuicer.resource_lock as resource_lock
import datajuicer.local_cache as local_cache
import datajuicer.cache as cache
import argparse
import os
import pathlib
import dill
import subprocess
import datetime
import shutil
import sys
from contextlib import redirect_stderr, redirect_stdout
import importlib


stack = threading.local()

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
        default = Keep
        if Keep in self.deps.values():
            default = Ignore

        new_kwargs = copy.copy(kwargs)
        for key in kwargs:
            if key in self.deps:
                action = self.deps[key]
            else:
                action = default
            if action == Ignore:
                del new_kwargs[key]
            elif type(action) is Depend:
                new_kwargs[key] = action.modify(new_kwargs[key])

        return new_kwargs

class Context:
    def __init__(self):
        self.context_id = utils.rand_id()
    attributes = (
        "task_name", 
        "file_dict", 
        "func_name_dict",
        "version_dict",
        "cache_dict",
        "force",
        "incognito",
        "args",
        "kwargs",
        "run_id",
        "result",
        "dependencies",
        "session_id",
        "context_id",
        "parent"
    )

    def __setattr__(self, name: str, value) -> None:
        if not name in Context.attributes and not name in Context.__dict__:
            raise Exception("invalid attribute")
        if name in self.__dict__:
            raise Exception(f"cannot assign to field {name!r}")
        else:
            self.__dict__[name] = value

    def get_func(self):
        file = self.file_dict[self.task_name]
        sys.path.append(os.path.dirname(file))
        module = importlib.import_module(pathlib.Path(file).stem)
        return module.__dict__[self.func_name_dict[self.task_name]]

class Run:
    def __init__(self, context, launcher):
        self.context = context
        self.cond = threading.Condition()
        self.launcher = launcher

        class NullContextManager(object):
            def __init__(self, dummy_resource=None):
                self.dummy_resource = dummy_resource
            def __enter__(self):
                return self.dummy_resource
            def __exit__(self, *args):
                pass
        
        condition = self.cond
        name = context.task_name
        func = context.get_func()
        version = context.version_dict[name]
        cache = context.cache_dict[name]
        boundargs = inspect.signature(func).bind(*context.args,**context.kwargs)
        boundargs.apply_defaults()
        kwargs = dict(boundargs.arguments)
        self.all_kwargs = kwargs
        dependencies = Depend(**context.dependencies).modify(kwargs)

        

        rl = resource_lock.ResourceLock(context.session_id, init=False)

        def check_run_deps(cache, name, version, rid):
            if context.version_dict[name] != version:
                return False
            rdeps = cache.get_run_dependencies(name, version, rid)
            for n, v, i in rdeps:
                if not check_run_deps(context.cache_dict[n], n, v, i):
                    return False
            return True
        
        def add_dep():
            if context.parent is not None:
                if not context.parent.incognito and not context.incognito:
                    cache.add_run_dependency(
                    context.parent.task_name, 
                    context.parent.version_dict[context.parent.task_name], 
                    context.parent.run_id, 
                    name, 
                    version, 
                    context.run_id)

        if not context.force:
            redo = False
            while(True):
                rids = cache.get_newest_runs(name, version, dependencies)
                for rid in rids:
                    
                    if check_run_deps(cache, name, version, rid):
                        rl.release()
                        while not cache.is_done(name, version, rid) and check_run_deps(cache, name, version, rid):
                            time.sleep(0.5)
                        rl.acquire()
                        if check_run_deps(cache, name, version, rid):
                            context.run_id = rid
                            add_dep()
                            with condition:
                                context.result = cache.get_result(name, version, rid)
                                condition.notify_all()
                            return
                        redo = True
                        break
                if redo:
                    continue
                new_rid = utils.rand_id()
                if context.incognito:
                    break
                success, rids = cache.conditional_record_run(name, version, new_rid, kwargs, dependencies, hash(rids))
                if success:
                    break
        else:
            new_rid = utils.rand_id()
            cache.record_run(name, version, new_rid, kwargs)
        context.run_id = new_rid
        add_dep()

        launcher.launch(context, self.cond)
    
    def join(self):
        rl = resource_lock.ResourceLock(_get_context().session_id, init=False)
        rl.release()
        with self.cond:
            while not hasattr(self.context, "result"):
                self.cond.wait()
        rl.acquire()
    
    def get(self):
        self.join()
        return self.context.result

    def open(self, path, mode):
        if any([x in mode for x in ["a", "w", "+"]]):
            raise Exception("Only Reading allowed")
        #self.join()
        return self._open(path, mode)

    def _open(self, path, mode):
        name = self.context.task_name
        version = self.context.version_dict[name]
        cache = self.context.cache_dict[name]
        return cache.open(name, version, self.context.run_id, path, mode)
    
    def __getitem__(self, item):
        return self.all_kwargs[item]

    def delete(self):
        name = self.context.task_name
        version = self.context.version_dict[name]
        cache = self.context.cache_dict[name]
        cache.delete_run(self.name, version, self.context.run_id)

class Launcher:
    pass
class Direct(Launcher):
    def __init__(self):
        pass

    def launch(self, context, condition=None):
        name = context.task_name
        version = context.version_dict[name]
        cache = context.cache_dict[name]
        
        func = context.get_func()
        

        boundargs = inspect.signature(func).bind(*context.args,**context.kwargs)
        boundargs.apply_defaults()
        
        import datajuicer as dj
        stack = dj.launch.stack
        if not hasattr(stack, "list"):
            stack.list = []
        stack.list.append(context)
        if not context.incognito:
        
            outlogger = Logger(cache.open(name, version, context.run_id, "log.txt", "w+"))
            errlogger = Logger(cache.open(name, version, context.run_id, "log.txt", "w+"), console="stderr")
            with redirect_stdout(outlogger):
                with redirect_stderr(errlogger):
                    res = func(*context.args,**context.kwargs)
            
            cache.record_result(name, version, context.run_id, res)
        else:
            res = func(*context.args,**context.kwargs)
        rl = resource_lock.ResourceLock(context.session_id, init=False)

        rl.free_all_resources()
        stack.list.pop()
        if condition is None:
            context.result = res
        else:
            with condition:
                context.result = res
                condition.notify_all()



class NewProcess(Launcher):

    class Thread(threading.Thread):
        def __init__(self, context, directory, condition = None):
            super().__init__()
            self.context = context
            self.directory = directory
            self.condition = condition
        
        def run(self):
            path = os.path.join(self.directory, f"{self.context.context_id}_result.dill")
            while not os.path.isfile(path):
                time.sleep(0.5)
            
            with open(path, "rb") as f:
                result = dill.load(f)
            
            if self.condition is None:
                self.context.result = result
            
            else:
                with self.condition:
                    self.context.result = result
                    self.condition.notify_all()

    def __init__(self, directory = "dj_resources"):
        self.directory = directory
    
    def prepare(self, context):
        path1 = pathlib.Path(os.path.join(self.directory, f"{context.context_id}_context.dill")).resolve()

        with open(path1, "wb+") as f:
            dill.dump(context, f)

        cls = type(self).__name__

        return f"python {pathlib.Path(__file__).resolve()} -path {path1} -launchmode {cls}"


    def launch(self, context, condition=None):

        command = self.prepare(context)
        subprocess.Popen(command.split())

        t = NewProcess.Thread(context, self.directory, condition)
        t.start()
    
    @staticmethod
    def start(args):
        print("in start")
        ap = argparse.ArgumentParser()
        ap.add_argument("-path", type=str)
        args = ap.parse_args(args)
        print("Dj launch %s" % args.path)
        with open(args.path, "rb") as f:
            context = dill.load(f)
        
        rl = resource_lock.ResourceLock(context.session_id, init=False)
        #print(f"made resource lock {rl.session}. value is {rl.workers_semaphore.value}")
        # print("ANORMAL RELEASE")
        # rl.release()
        rl.acquire()
        print("acquired")
        Direct().launch(context)
        result = context.result
        rl.release()

        directory = os.path.dirname(args.path)
        path = os.path.join(directory, f"{context.context_id}_result.dill")
        with open(path, "wb+") as f:
            dill.dump(result, f)

class Command(NewProcess):
    def __init__(self, template, directory = "dj_resources"):
        self.template = template
        super().__init__(directory)
    
    def launch(self, context, condition=None):
        command = self.prepare(context)
        command = self.template.replace("COMMAND", command)
        os.system(command)
        t = Command.Thread(context, self.directory, condition)
        t.start()
class NewThread(Launcher):

    class Thread(threading.Thread):
        def __init__(self, context, condition=None):
            super().__init__()
            self.context = context
            self.condition = condition
        
        def run(self):
            rl = resource_lock.ResourceLock(self.context.session_id, init=False)
            rl.acquire()
            Direct().launch(self.context, self.condition)
            rl.release()
            

    def __init__(self):
        pass

    def launch(self, context, condition=None):
        t = NewThread.Thread(context, condition)
        t.start()

class TaskList(object):
    def __init__(self):
        self.default_dependencies = {}
        self.default_launcher = {}
        self.file_dict = {}
        self.func_name_dict = {}
        self.cache_dict = {}
        self.version_dict = {}

    def add_task(self, func, name = None,version=0.0, cache=None, default_launcher=None, **default_dependencies):
        if name is None:
            name = func.__name__
        if cache is None:
            cache = local_cache.LocalCache()
        if default_launcher is None:
            default_launcher = Direct()
        self.default_dependencies[name] = default_dependencies
        self.func_name_dict[name] = func.__name__
        self.file_dict[name] = func.__globals__['__file__']
        self.cache_dict[name] = cache
        self.version_dict[name] = version
        self.default_launcher[name] = default_launcher
    
    def __getattr__(self, name):
        if name in self.__dict__:
            return super(TaskList, self).__getattr__(name)
        return LaunchTemplate(self, name)
    
    def __call__(self, name=None, *args, **kwargs):
        def wrapper(func):
            self.add_task(func, name, *args, **kwargs)
            return func
        return wrapper



class Session:
    def __init__(self, max_workers, **resources):
        self.session_id = utils.rand_id()
        rl = resource_lock.ResourceLock(self.session_id, init = True)
        for _ in range(max_workers):
            rl.release()
        rl.free_resources(**resources)


class LaunchTemplate:

    def __init__(self, task_list, task_name):
        self.task_list = task_list
        self.task_name = task_name
        self._force = False
        self._incognito = False
        self.dependencies = task_list.default_dependencies[task_name]
        self.launcher = task_list.default_launcher[task_name]
        self.session_id = None
        import datajuicer as dj
        stack = dj.launch.stack
        if hasattr(stack, "list"):
            if len(stack.list) > 0:
                self.session_id = stack.list[-1].session_id
            
        
        
    def with_launcher(self, launcher):
        self.launcher = launcher
        return self
    
    def in_new_session(self, max_workers, **resources):
        session = Session(max_workers, **resources)
        self.session_id = session.session_id
        return self

    def with_dependencies(self, **dependencies):
        self.dependencies = dependencies
        return self
    
    def force(self):
        self._force = True
        return self
    
    def incognito(self):
        self._incognito = True
        return self
    
    def __call__(self, *args, **kwargs):

        force = self._force
        incognito = self._incognito
        # if "force" in kwargs:
        #     force = kwargs["force"]
        #     del kwargs["force"]
        # if "incognito" in kwargs:
        #     incognito = kwargs["incognito"]
        #     del kwargs["incognito"]

        context = Context()
        context.task_name = self.task_name
        context.file_dict = self.task_list.file_dict
        context.func_name_dict = self.task_list.func_name_dict
        context.version_dict = self.task_list.version_dict
        context.cache_dict = self.task_list.cache_dict
        context.force = force
        context.incognito = incognito
        context.args = args
        context.kwargs = kwargs
        context.dependencies = self.dependencies
        context.session_id = self.session_id
        context.parent = _get_context()

        return Run(context, self.launcher)



class Launcher:
    pass


class Logger:
 
    def __init__(self, file, mute = False, console="stdout"):
        self.console = getattr(sys,console)
        self.file = file
        self.mute= mute
 
    def write(self, message):
        if not self.mute:
            self.console.write(message)
        self.file.write(message)
 
    def flush(self):
        self.console.flush()
        self.file.flush()


def _get_context():
    import datajuicer as dj
    stack = dj.launch.stack
    if not hasattr(stack, "list"):
        return None
    if len(stack.list) == 0:
        return None
    return stack.list[-1]

def run_id():
    return _get_context().run_id

def backup():
    now = datetime.now()
    if os.path.exists("dj_backups/"):
        shutil.rmtree("dj_backups/")
    cache.make_dir("dj_backups/")
    local_cache.LocalCache().save(os.path.join("dj_backups", now.strftime("%Y-%m-%d-%H-%M-%S.backup")))

def sync_backups():
    cache.make_dir("dj_backups/")
    _cache = local_cache.LocalCache()
    for filename in os.listdir("dj_backups"):
        _cache.update(os.path.join("dj_backups", filename))

def clean():
    local_cache.LocalCache().clean()

def _open(path, mode):
    context = _get_context()
    name = context.task_name
    version = context.version_dict[name]
    cache = context.cache_dict[name]
    return cache.open(name, version, context.run_id, path, mode)

def reserve_resources(**resources):
    rl = resource_lock.ResourceLock(_get_context().session_id, init=False)
    rl.reserve_resources(**resources)

def free_resources(**resources):
    rl = resource_lock.ResourceLock(_get_context().session_id, init=False)
    rl.free_resources(**resources)

def setup(max_workers, clean=False, **resources):
    s = Session(max_workers, **resources)
    c = Context()
    c.session_id = s.session_id
    c.incognito = True
    import datajuicer as dj
    stack = dj.launch.stack
    stack.list = []
    stack.list.append(c)
    rl = resource_lock.ResourceLock(_get_context().session_id, init=False)
    rl.acquire()
    if clean:
        dj.launch.clean()


if __name__ == "__main__":
    print("in  main")
    ap = argparse.ArgumentParser()
    ap.add_argument("-launchmode", type=str)
    args, remaining = ap.parse_known_args()
    launchmode = eval(args.launchmode)
    launchmode.start(remaining)