from datajuicer.core.active import Inactive, get_active
from datajuicer.errors import  NoActiveRunException
from datajuicer.interface.states import get_cache, cache_type, run_directory, resource_pool, tmp_directory
import os
from datajuicer.cache.cache import Cache


def set_cache_type(type):
    """Set the cache type to use for the current run. Must be a subclass of Cache.
    Args:
        type (Cache): The cache type to use for the current run.
    """
    # check if type is valid
    if not issubclass(type, Cache):
        raise Exception("Cache type must be a subclass of Cache")
    cache_type.set(type)

def set_directory(directory):
    """Set the main directory for the project. This is where all the runs and temporary files will be stored.

    Args:
        directory (str): The directory.
    """
    run_directory.set(os.path.join(directory, "dj_runs"))
    tmp_directory.set(os.path.join(directory, "tmp"))

def load_runs_from_disk():
    """Load all runs from disk into the cache. This is necessary if you have copied runs into the run directory from another machine.
    """
    get_cache().load_from_disk()

def open(path, mode):
    """Open a file in the user_files directory for the current run. This is a wrapper around the built-in open function.

    Args:
        path (str): The path to the file, relative to the user_files directory.
        mode (str): The mode to open the file in.

    Raises:
        NoActiveRunException: If there is no active run.

    Returns:
        file: The file object.
    """
    execution = get_active("execution")
    if execution is Inactive:
        raise NoActiveRunException

    return execution.open(os.path.join("user_files", path), mode)

def reserve_resources(**resources):
    """Reserve resources for the current run. This is used to prevent other runs from using the same resources. The resources are freed when the run finishes. The resources can be any keyword arguments, for example "gpu", "ram" and should be floats or ints. Deadlocks are prevented by always reserving resources in the same order.

    Raises:
        NoActiveRunException: If there is no active run.

    Returns:
        None: None
    """
    execution = get_active("execution")
    if execution is Inactive:
        raise NoActiveRunException
    return resource_pool.get().reserve_resources(execution.run_id, **resources)

def free_resources(**resources):
    """Free resources for the current run. This is used to free resources that were reserved with reserve_resources.

    Returns:
        None: None
    """

    execution = get_active("execution")
    if execution is Inactive:
        return resource_pool.get().free_global_resources(**resources)
    return resource_pool.get().free_resources(execution.run_id, **resources)

def current_run_id():
    """Get the run_id of the current run.

    Raises:
        NoActiveRunException: If there is no active run.

    Returns:
        run_id (str): The run_id of the current run.
    """
    execution = get_active("execution")
    if execution is Inactive:
        raise NoActiveRunException
    return execution.run_id





