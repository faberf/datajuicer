from datajuicer.core.active import Inactive, get_active
from datajuicer.errors import  NoActiveRunException
from datajuicer.interface.states import get_cache, cache_type, run_directory, resource_pool, tmp_directory
import os

def set_cache_type(type):
    cache_type.set(type)

def set_directory(directory):
    run_directory.set(os.path.join(directory, "dj_runs"))
    tmp_directory.set(os.path.join(directory, "tmp"))

def load_runs_from_disk():
    get_cache().load_from_disk()

def open(path, mode):
    execution = get_active("execution")
    if execution is Inactive:
        raise NoActiveRunException

    return execution.open(os.path.join("user_files", path), mode)

def reserve_resources(**resources):
    execution = get_active("execution")
    if execution is Inactive:
        raise NoActiveRunException
    return resource_pool.get().reserve_resources(execution.run_id, **resources)

def free_resources(**resources):

    execution = get_active("execution")
    if execution is Inactive:
        return resource_pool.get().free_global_resources(**resources)
    return resource_pool.get().free_resources(execution.run_id, **resources)

def current_run_id():
    execution = get_active("execution")
    if execution is Inactive:
        raise NoActiveRunException
    return execution.run_id





