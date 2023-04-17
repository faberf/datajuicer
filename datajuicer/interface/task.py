
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
        params = task.params_query.extract(),
        function = task.func,
        worker_semaphore=worker_semaphore.get(),
        resource_pool=resource_pool.get(),
        tmp_directory=tmp_directory.get()
    )
    exec.launch(launcher,last_hash=last_hash)
    return exec

class Task:
    """A task is a wrapper around a function that allows the user to run the function and get the result. The function is not rerun if a suitable run is found in the cache. The task also allows the user to run the function in the background and join the task later. The task also allows the user to open files that were created by the function. The task also allows the user to get the console output of the function. 
    """
    def __init__(self, func, *args, **kwargs):
        """Create a new task.

        Args:
            func (callable): The function that the task should wrap.
            args (tuple): The positional arguments that should be passed to the function.
            kwargs (dict): The keyword arguments that should be passed to the function.
        """
        self.func = Function(func)
        self.params_query = make_query(apply_defaults(func, *args, **kwargs))
        self.run_obj = None
        
    def run(self, launcher=Direct()):
        """Launch the function if no suitable run is found in the cache and have it run as soon as a worker is available. If a suitable run is found in the cache, that run is loaded. Depending on the launcher passed to this function, the function may run immediately or in a new thread, a new Process or on a remote machine.

        Args:
            launcher (Launcher, optional): The launcher. Defaults to Direct().
        """
        while(True): #TODO: Are there race conditions?
            last_hash = get_cache().get_hash()
            self.find(acceptable_states=[RunState.Complete])
            if self.run_obj:
                return
            self.find()
            if self.run_obj:
                return
            try:
                self.run_obj = _force(self, launcher, last_hash)
            except InvalidHashException as e:
                continue
            return
    
    def join(self, tick_every=0.1):
        """Sleep until the function has finished running. Similar to get() but does not return the result. If the function has not been run yet, raise an exception.

        Args:
            tick_every (float, optional): The time to sleep between checks. Defaults to 0.1.

        Raises:
            PrematureJoinException: If the function has not been run yet. This can occur if find() does not find a suitable run.

        Returns:
            self (Task): The task object.
        """
        if self.run_obj is None:
            raise PrematureJoinException
        worker_semaphore.get().release()
        while not self.run_obj.get_state() is RunState.Complete:
            time.sleep(tick_every)
        worker_semaphore.get().acquire()
        return self
    
    def get(self, tick_every=0.1):
        """Sleep until the function has finished running and return the result. Similar to join(). If a suitable run is found in the cache, the function is not rerun. Depending on the launcher passed to this function, the function may run immediately or in a new thread, a new Process or on a remote machine. This function will block until the function has finished running.

        Args:
            tick_every (float, optional): The time to sleep between checks. Defaults to 0.1.

        Returns:
            result (object): The result of the function.
        """
        self.join(tick_every=tick_every)
        return self.run_obj.get_result()
    
    def open(self, path, mode):
        """Open a file that was created by the function. If the function has not been run yet, raise an exception.

        Args:
            path (str): The path to the file.
            mode (str): The mode to open the file in. Should be "r" or "rb".

        Raises:
            PrematureJoinException: If the function has not been run yet. This can occur if find() does not find a suitable run.
            IllegalWriteException: If the mode is not "r" or "rb".

        Returns:
            file (file): The file object.
        """
        if self.run_obj is None:
            raise PrematureJoinException
        if not "r" in mode:
            raise IllegalWriteException
        return self.run_obj.open(os.path.join("user_files", path), mode)
    
    def get_log(self):
        """Get the console output of the function. If the function has not been run yet, raise an exception.

        Raises:
            PrematureJoinException: If the function has not been run yet. This can occur if find() does not find a suitable run.

        Returns:
            log (str): The console output of the function.
        """        """"""
        if self.run_obj is None:
            raise PrematureJoinException
        return self.run_obj.get_log()


    def find(self, acceptable_states = [RunState.Complete, RunState.Alive, RunState.Pending], sort_oldest=False):
        """Find a suitable run in the cache. If a suitable run is found, the run is loaded. If multiple suitable runs are found, the most recent one is loaded.
        
        Args:
            acceptable_states (list, optional): The acceptable states of the run. Defaults to [RunState.Complete, RunState.Alive, RunState.Pending]
            sort_oldest (bool, optional): If True, find the oldest matching run. Otherwise find the most recent matching run. Defaults to False.
        """
        cache = get_cache() # We use the global cache here because we do not have a run object yet.
        found = find(cache, self.func, self.params_query, acceptable_states=acceptable_states, return_all=False, sort_oldest=sort_oldest)
        if found is NoRuns:
            found = None
        self.run_obj = found
        
    def find_all(self, acceptable_states = [RunState.Complete, RunState.Alive, RunState.Pending], sort_oldest=False):
        """Find all suitable runs in the cache. 
        
        Args:
            acceptable_states (list, optional): The acceptable states of the run. Defaults to [RunState.Complete, RunState.Alive, RunState.Pending]
            sort_oldest (bool, optional): If True, sort the runs from oldest to newest. Otherwise sort from newest to oldest. Defaults to False.
        
        Returns:
            list (list): A list of Task objects.
        """
        
        cache = get_cache()
        found = find(cache, self.func, self.params_query, acceptable_states=acceptable_states, return_all=True, sort_oldest=sort_oldest)
        ret = []
        for run in found:
            new_task = Task.__new__(Task)
            new_task.func = self.func
            new_task.params_query = self.params_query
            new_task.run_obj = run
            ret.append(new_task)
        return ret
    
    def force(self, launcher=Direct()):
        """RLaunch the function have it run as soon as a worker is available. The cache is ignored. Depending on the launcher passed to this function, the function may run immediately or in a new thread, a new Process or on a remote machine.

        Args:
            launcher (_type_, optional): _description_. Defaults to Direct().
        """
        self.run_obj = _force(self, launcher, None)
        


