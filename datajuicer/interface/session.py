from datajuicer.interface.states import make_worker_semaphore, tmp_directory, resource_pool, worker_semaphore
from datajuicer.ipc.resource_pool import ResourcePool
from datajuicer.utils import make_id
from contextlib import nested

class Session:

    def __init__(self, n_workers):
        resource_context = resource_pool.context(ResourcePool(tmp_directory.get(), make_id()))
        worker_context = worker_semaphore.context(make_worker_semaphore(n_workers))
        context = nested(resource_context, worker_context)
        self.__enter__ = context.__enter__
        self.__exit__ = context.__exit__
