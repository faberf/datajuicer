from datajuicer.cache.tinydbcache import TinyDBCache
from datajuicer.core.active import State
from datajuicer.ipc.resource_pool import ResourcePool
from datajuicer.ipc.semaphore import Semaphore
from datajuicer.utils import make_id

def make_worker_semaphore(n_workers):
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
    return cache_type.get()(run_directory.get())