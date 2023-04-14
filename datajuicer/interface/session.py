from datajuicer.interface.states import make_worker_semaphore, tmp_directory, resource_pool, worker_semaphore
from datajuicer.ipc.resource_pool import ResourcePool
from datajuicer.utils import make_id
from contextlib import ExitStack



class Session(ExitStack):
    """This class is used to create a new pool of workers and resources for a set of tasks. This is useful for when you want to run a set of tasks in parallel, but you don't want to have to worry about the tasks interfering with each other. For example, when you are executing on a cluster, each machine can use its own session. The machines will still share the same runs and caching functionality, however they will execute completely in parallel. This class is a context manager, so you can use it with the `with` statement. When you exit the `with` statement, you will return to the previous session or the default session if there was no previous session."""
    def __init__(self, n_workers):
        """Create a new session.

        Args:
            n_workers (int): The maximum number of workers to use in this session.
        """
        super().__init__()
        self.resource_context = resource_pool.context(ResourcePool(tmp_directory.get, make_id()))
        self.worker_context = worker_semaphore.context(make_worker_semaphore(n_workers))
    
    def __enter__(self):
        """Enter the session. This will create a new pool of workers and resources.
        """
        ret = super().__enter__()
        worker_semaphore.get().release() #TODO: more principled approach?
        self.enter_context(self.resource_context)
        self.enter_context(self.worker_context)
        return ret
    
    def __exit__(self, __exc_type, __exc_value, __traceback) -> bool:
        worker_semaphore.get().release()
        ret = super().__exit__(__exc_type, __exc_value, __traceback)
        worker_semaphore.get().acquire()
        return ret
