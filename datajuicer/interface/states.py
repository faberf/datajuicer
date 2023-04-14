from datajuicer.cache.tinydbcache import TinyDBCache
from datajuicer.core.active import State
from datajuicer.ipc.resource_pool import ResourcePool
from datajuicer.ipc.semaphore import Semaphore
from datajuicer.utils import make_id

"""Here we define the global state of datajuicer. This is used to store the default values for the various parameters that are used throughout the library. This is also used to store the global state of the library, such as the resource pool and the worker semaphore. This is done so that the user does not have to pass these parameters around. The user can modify the global state by using the global interface or using sessions.
"""

def make_worker_semaphore(n_workers):
    """Create a new worker semaphore.

    Args:
        n_workers (int): The maximum number of workers that the semaphore should allow.

    Returns:
        worker_semaphore (Semaphore): The new worker semaphore.
    """
    worker_semaphore = Semaphore(tmp_directory.get, make_id())
    for _ in range(n_workers-1):
        worker_semaphore.release()
    return worker_semaphore


DEFAULT_N_WORKERS  = 1
DEFAULT_TMP_DIRECTORY = "tmp"
DEFAULT_RUN_DIRECTORY = "dj_runs"
DEFAULT_CACHE_TYPE = TinyDBCache

tmp_directory = State("tmp_directory", DEFAULT_TMP_DIRECTORY, True)
worker_semaphore = State("worker_semaphore", make_worker_semaphore(DEFAULT_N_WORKERS), True)
resource_pool = State("resource_pool", ResourcePool(tmp_directory.get, make_id()), True)
run_directory = State("run_directory", DEFAULT_RUN_DIRECTORY, True)
cache_type = State("cache_type", DEFAULT_CACHE_TYPE, True)

def get_cache():
    """Helper function to get the cache from the global state.

    Returns:
        cache (Cache): The cache.
    """
    return cache_type.get()(run_directory.get())