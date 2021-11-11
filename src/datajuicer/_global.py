import threading
import datajuicer as dj

class GLOBAL:
    resource_lock = dj.ResourceLock()
    cache = dj.LocalCache()


def setup(max_n_threads=None, cache=None, **max_resources):
    GLOBAL.resource_lock = dj.ResourceLock(max_n_threads, max_resources)
    if not cache is None:
        GLOBAL.cache = cache



def run_id():
    return threading.currentThread().run_id

def reserve_resources(**resources):
    threading.currentThread().resource_lock.reserve_resources(**resources)

def free_resources(**resources):
    threading.currentThread().resource_lock.free_resources(**resources)

def _open(path, mode):
    t = threading.currentThread()
    if not hasattr(t, "open"):
        return open(path, mode)
    return t._open(path, mode)