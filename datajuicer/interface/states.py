from datajuicer.cache.tinydbcache import TinyDBCache
from datajuicer.core.active import State
from datajuicer.ipc.resource_pool import ResourcePool
from datajuicer.ipc.semaphore import Semaphore
from datajuicer.utils import make_id

def make_worker_semaphore(n_workers):
    worker_semaphore = Semaphore(tmp_directory.get(), make_id())
    for _ in range(n_workers-1):
        worker_semaphore.release()

def identity(x):
    return x

DEFAULT_N_WORKERS  = 1
DEFAULT_TMP_DIRECTORY = "tmp"
DEFAULT_RUN_DIRECTORY = "dj_runs"
DEFAULT_RESOURCES = {}
DEFAULT_CACHE_TYPE = TinyDBCache

tmp_directory = State(identity, "tmp_directory", DEFAULT_TMP_DIRECTORY, False)
worker_semaphore = State(make_worker_semaphore, "worker_semaphore", DEFAULT_N_WORKERS, True)
resource_pool = State(identity, "resource_pool", ResourcePool(tmp_directory.get(), make_id()), True)
run_directory = State(identity, "run_directory", DEFAULT_RUN_DIRECTORY, True)
cache_type = State(identity, "cache_type", DEFAULT_CACHE_TYPE, True)

def get_cache():
    return cache_type.get()(run_directory.get())