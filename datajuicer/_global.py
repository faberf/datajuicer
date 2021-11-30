import threading
import datajuicer.resource_lock
import datajuicer.local_cache
import datajuicer.logging 
import os
from datetime import datetime

class GLOBAL:
    resource_lock = None
    cache = None
    tasks = {}


def setup(max_n_threads=1, cache=None, **max_resources):
    datajuicer.logging.enable_proxy()
    GLOBAL.resource_lock = datajuicer.resource_lock.ResourceLock(max_n_threads, **max_resources)
    if cache is None:
        cache = datajuicer.local_cache.LocalCache()
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

def backup():
    now = datetime.now()
    GLOBAL.cache.save(os.path.join("dj_backups", now.strftime("%Y-%m-%d-%H-%M-%S.backup")))

def sync_backups():
    datajuicer.cache.make_dir("dj_backups/")
    for filename in os.listdir("dj_backups"):
        GLOBAL.cache.update(os.path.join("dj_backups", filename))