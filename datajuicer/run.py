

from json import JSONDecodeError
import time
import dill as pickle
import ujson as json
import datajuicer.dependency as dependency
from contextlib import redirect_stderr, redirect_stdout
import os
import datajuicer.utils as utils
import sys


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


JOIN_TICK = 1.0

class Run:

    @staticmethod
    def make(cache, incognito, task_name, parameters, init_cooldown):
        from datajuicer.context import  Context
        run = Run(utils.rand_id(), cache)
        cache.new_run(run.run_id)
        run.add_dependency(dependency.TaskNameDependency(task_name))
        run.add_dependency(dependency.DoneDependency())
        run.add_dependency(dependency.ParamDependency(parameters))
        if incognito:
            run.add_dependency(dependency.ImpossibleDependency())
        elif not Context.get_active().incognito:
            Context.get_active().run.add_dependency(dependency.RunDependency(run))
        
        run.record_initialized(init_cooldown)
        return run

    def __init__(self, run_id, cache):
        self.run_id = run_id
        self.cache = cache
    
    def join(self):
        from datajuicer.context import  Context
        con = Context.get_active()
        con.resource_lock.release()
        while True:
            state = self.get_state()
            if state == "crashed" or state == "crashed_init":
                if self.cache.exists(self.run_id, "exception.pickle"):
                    with self._open("exception.pickle", "rb") as f:
                        raise pickle.load(f)
                raise Exception("Run appears to have crashed.")
            if state == "done":
                con.resource_lock.acquire()
                return self
            time.sleep(JOIN_TICK)

    def redirect_output(self):
        class Context:
            def __init__(self, log_file):
                self.log_file = log_file
            def __enter__(self):
                self.log_file.__enter__()

                outlogger = Logger(self.log_file)
                errlogger = Logger(self.log_file, console="stderr")

                self.redirect_stdout = redirect_stdout(outlogger)
                self.redirect_stderr = redirect_stderr(errlogger)
                self.redirect_stdout.__enter__()
                self.redirect_stderr.__enter__()
            
            def __exit__(self, exc_type, exc_value, exc_traceback):

                self.redirect_stderr.__exit__(exc_type, exc_value, exc_traceback)
                self.redirect_stdout.__exit__(exc_type, exc_value, exc_traceback)
                self.log_file.__exit__(exc_type, exc_value, exc_traceback)
        
        return Context(self._open("log.txt", "w+"))

    def open_result(self, write = False):
        mode = "br"
        if write:
            mode = "bw+"
        return self._open( "result.pickle", mode)
    
    def open_exception(self, write = False):
        mode = "br"
        if write:
            mode = "bw+"
        return self._open( "exception.pickle", mode)

    def get(self):
        self.join()
        with self.open_result(write = False) as f:
            return pickle.load(f)
        
        
    def add_dependency(self, dependency):
        dep_list = []
        if self.exists("deps.pickle"):
            with self._open("deps.pickle", "rb") as f:
                dep_list = pickle.load(f)
        dep_list.append(dependency)
        with self._open("deps.pickle", "wb+") as f:
            pickle.dump(dep_list, f)
    
    def _open_state(self, mode):
        return self.cache._open(self.run_id, "state.json", mode)

    def _state_exists(self):
        return self.exists("state.json")
    
    def exists(self, path=None):
        return self.cache.exists(self.run_id, path)

    def load_deps(self):
        if not self.exists("deps.pickle"):
            return []
        with self._open("deps.pickle", "rb") as f:
            return pickle.load(f)
    
    def record_alive(self, cooldown):
        with self.cache.get_lock():
            t = time.time()
            with self._open_state("w+") as f:
                ss = json.dumps(("alive",t, cooldown))
                f.write(ss)

    def record_initialized(self, cooldown):
        with self.cache.get_lock():
            t = time.time()
            with self._open_state("w+") as f:
                ss = json.dumps(("initialized",t, cooldown))
                f.write(ss)
    
    def _get_state(self):
        if not self._state_exists():
            return "crashed"
        with self._open_state("r") as f:
            ss = f.read()
            try:
                state, then, duration = json.loads(ss)
            except json.JSONDecodeError as ex:
                try:
                    state, then, duration = json.loads(ss[0:ss.index(']')+1])
                except:
                    print(f"WARNING: Run {self.run_id} error loading state")
                    return "crashed"
        
        now = time.time()
        expired = now - then > duration

        if expired and state == "initialized":
            state = "crashed_init"
        if expired and state == "alive":
            state = "crashed"

        return state

    def get_state(self):
        with self.cache.get_lock():
            return self._get_state()

    def check_dependencies(self, request):
        return dependency._check_deps(self.load_deps(), object, self, request)
    
    def check_global_dependencies(self):
        return dependency._check_deps(self.load_deps(), dependency.GlobalDependency, self, None)

    def _open(self, path, mode):
        return self.cache.open(self.run_id, path, mode)
    
    def _open_inner(self, path, mode):
        return self._open(os.path.join("user_files", path), mode)


    def open(self, path, mode):
        self.join()
        assert("r" in mode)
        return self._open_inner(path, mode)

    def record_exception(self, exception):
        with self.cache.get_lock():
            with self._open_state("w+") as f:
                ss = json.dumps(("crashed",time.time(), -1))
                f.write(ss)
        
        with self.open_exception(write = True) as f:
            pickle.dump(exception, f)
    
    def record_done(self, result):
        with self.cache.get_lock():
            with self._open_state("w+") as f:
                ss = json.dumps(("done",time.time(), -1))
                f.write(ss)
        
        with self.open_result(write = True) as f:
            pickle.dump(result, f)
        
    
    def record_terminated(self):
        with self.cache.get_lock():
            with self._open_state("w+") as f:
                ss = json.dumps(("terminated", time.time(),-1))
                f.write(ss)

    def delete(self):
        self.cache.delete(self.run_id)

    def get_log(self):
        with self._open("log.txt","r") as f:
            return f.read()
