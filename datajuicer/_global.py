import multiprocessing
import threading
import datajuicer.resource_lock
import datajuicer.local_cache
import datajuicer.logging 
import os
from datetime import datetime
import sys
class GLOBAL:
    resource_lock = None
    cache = None
    task_versions = {}
    task_caches = {}


def setup(cache=None, max_workers=1, resource_directory="dj_resources/"):
    #multiprocessing.set_start_method('spawn')
    curthread = threading.current_thread()
    assert(type(curthread) != datajuicer.task.Run)
    curthread.unique_id = datajuicer.utils.rand_id()
    datajuicer.logging.enable_proxy()
    GLOBAL.resource_lock = datajuicer.resource_lock.ResourceLock(resource_directory, True)
    for i in range(max_workers-1):
        GLOBAL.resource_lock.release()
    # if max_workers > 1:
    #     GLOBAL.resource_lock.release(max_workers-1)
    if cache is None:
        cache = datajuicer.local_cache.LocalCache()
    GLOBAL.cache = cache

# def subprocess_setup(cache, resource_directory):
#     curthread = threading.current_thread()
#     assert(type(curthread) != datajuicer.task.Run)
#     curthread.unique_id = datajuicer.utils.rand_id()
#     datajuicer.logging.enable_proxy()
#     GLOBAL.resource_lock = datajuicer.resource_lock.ResourceLock(resource_directory, False)
#     GLOBAL.cache = cache



def run_id():
    cur_thread = threading.currentThread()
    if type(cur_thread) is datajuicer.task.Run:
        return threading.currentThread().run_id
    return "main"

def reserve_resources(**resources):
    threading.currentThread().resource_lock.reserve_resources(**resources)

def free_resources(**resources):
    cur_thread = threading.currentThread()
    if type(cur_thread) is datajuicer.task.Run:
        threading.currentThread().resource_lock.free_resources(**resources)
    else:
        GLOBAL.resource_lock.free_resources(**resources)

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