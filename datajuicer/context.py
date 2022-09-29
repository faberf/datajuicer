import threading
import time
from datajuicer.lookup import LookUp
from datajuicer.run import Run
import datajuicer.launch as launch
from datajuicer.scratch import ScratchSpace
import dill as pickle
import pathlib
import os

class AliveTickThread(threading.Thread):
    def __init__(self, context, lock):
        super().__init__()
        self.context = context
        self.parent = threading.current_thread()
        self.lock = lock
    
    def run(self):

        while self.parent.is_alive() and not self.context.run.get_state() in ["done", "terminated"]:
            with self.lock:
                self.context.run.record_alive(self.context.alive_cooldown)
            time.sleep(self.context.tick_every)

if not __name__ == "__main__":
    active = threading.local()
    active.context = None

class BaseContext:
    def __init__(self, run, resource_lock, scratch_space, incognito, lookup):
        self.run = run 
        self.resource_lock = resource_lock
        self.scratch = scratch_space
        self.incognito = incognito
        self.lookup = lookup
    
    
    def all_runs(self):
        return [Run(rid, self.run.cache) for rid in self.run.cache.all_runs()]

def setup(run_dir = "dj_runs", scratch_dir = "dj_scratch", max_workers = 1, **resources):
    from datajuicer.session_mode import NewSession
    import datajuicer.utils as utils
    from datajuicer.cache import SimpleCache
    from datajuicer.dependency import ImpossibleDependency
    scratch = ScratchSpace(scratch_dir)
    rl = NewSession(max_workers, **resources).make_resource_lock(scratch)
    run = Run(utils.rand_id(), SimpleCache(run_dir))
    #run.cache.new_run(run.run_id)
    #run.add_dependency(ImpossibleDependency())
    lookup = LookUp(scratch)



    cont = BaseContext(run ,rl, scratch, incognito = True, lookup=lookup)
    active.context = cont
    rl.acquire()


class Context(BaseContext):
    def __init__(self, run, resource_lock, scratch_space, incognito, lookup, request, tick_every, alive_cooldown):
        super().__init__(run, resource_lock, scratch_space, incognito, lookup)
        self.request = request
        self.tick_every= tick_every
        self.alive_cooldown = alive_cooldown
    
    @staticmethod
    def get_active():
        if not hasattr(active, "context") or active.context is None:
            setup()

        return active.context
    
    def execute(self):
        l = threading.Lock()
        AliveTickThread(self, l).start()

        self.parent = active.context
        active.context = self
        self.resource_lock.acquire()
        with self.run.redirect_output():
            try:
                res = self.request.execute()
            except Exception as e:
                with l:
                    self.run.record_exception(e)
                raise e
        with l:
            self.run.record_done(res)
        self.resource_lock.free_all_resources(self.run.run_id)
        
        self.resource_lock.release()
        active.context = self.parent

    def get_path(self):
        return self.scratch.get_file_data(f"{self.run.run_id}_context", binary=True).path

    def to_disk(self):
        self.scratch.get_file_data(f"{self.run.run_id}_context",binary=True).set(pickle.dumps(self))

    def get_command(self):
        path = self.get_path()
        file = pathlib.Path(launch.__file__)
        return f"python {file} -path {path}"

def load():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("-path")
    args = ap.parse_args()
    with open(args.path, "rb") as f:
        return pickle.load(f)

def open_(path, mode):
    cont = Context.get_active()
    return cont.run._open_inner(path, mode)

def run_id():
    cont = Context.get_active()
    return cont.run.run_id

def reserve_resources(**resources):
    cont = Context.get_active()
    cont.resource_lock.reserve_resources(cont.run.run_id, **resources)
def free_resources(**resources):
    cont = Context.get_active()
    cont.resource_lock.free_resources(cont.run.run_id, **resources)