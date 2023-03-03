from datajuicer.interface.states import make_worker_semaphore, tmp_directory, resource_pool, worker_semaphore
from datajuicer.ipc.resource_pool import ResourcePool
from datajuicer.utils import make_id
from contextlib import ExitStack


class Session(ExitStack):

    def __init__(self, n_workers):
        super().__init__()
        self.resource_context = resource_pool.context(ResourcePool(tmp_directory.get, make_id()))
        self.worker_context = worker_semaphore.context(make_worker_semaphore(n_workers))
    
    def __enter__(self):
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
