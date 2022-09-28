
import os
import time
from datajuicer.cache.equality_query import make_query
from datajuicer.core.execution import Execution
from datajuicer.errors import IllegalWriteException, InvalidHashException, PrematureJoinException
from datajuicer.interface.global_interface import get_cache
from datajuicer.interface.states import worker_semaphore, resource_pool, tmp_directory
from datajuicer.ipc.function import Function
from datajuicer.core.launcher import Direct
from datajuicer.core.find import NoRuns, find
from datajuicer.core.run import RunState
from datajuicer.utils import apply_defaults

def _force(task, launcher, last_hash):
    exec = Execution(
        cache = get_cache(),
        function = task.func,
        worker_semaphore=worker_semaphore.get(),
        resource_pool=resource_pool.get(),
        tmp_directory=tmp_directory()
    )
    exec.launch(launcher,last_hash=last_hash)
    return exec

class Task:
    def __init__(self, func, *args, **kwargs):
        self.func = Function(func)
        self.params_query = make_query(apply_defaults(func, *args, **kwargs))
        self.run_obj = None
        
    def run(self, launcher=Direct()):
        while(True):
            last_hash = get_cache.get_hash()
            run_obj = self.find(acceptable_states=[RunState.Complete])
            if not run_obj is NoRuns:
                self.run_obj = run_obj
                return
            run_obj = self.find()
            if not run_obj is NoRuns:
                self.run_obj = run_obj
                return
            try:
                self.run_obj = _force(self, launcher, last_hash)
            except InvalidHashException as e:
                continue
    
    def join(self, tick_every=0.1):
        if self.run_obj is None:
            raise PrematureJoinException
        
        while not self.run_obj.get_state() is RunState.Complete:
            time.sleep(tick_every)
        return self
    
    def get(self):
        self.join()
        self.run_obj.get_result()
    
    def open(self, path, mode):
        if self.run_obj is None:
            raise PrematureJoinException
        if not "r" in mode:
            raise IllegalWriteException
        return self.run_obj.open(os.path.join("user_files", path), mode)
    
    def get_log(self):
        if self.run_obj is None:
            raise PrematureJoinException
        return self.run_obj.get_log()


    def find(self, acceptable_states = [RunState.Complete, RunState.Alive, RunState.Pending], return_all=False):
        cache = get_cache()
        self.run_obj = find(cache, self.func, self.params_query, acceptable_states=acceptable_states, return_all=return_all)
    
    def force(self, launcher=Direct()):
        self.run_obj = _force(self, launcher, None)
        


