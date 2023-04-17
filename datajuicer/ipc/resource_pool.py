import os
import time
import ujson as json
from datajuicer.ipc.constants import CHECK_INTERVAL

from datajuicer.ipc.file import File
from datajuicer.ipc.lock import Lock, NoParent




def move(amount,source=None, to=None):
    """Helper function to move resources from one dictionary to another.

    Args:
        amount (dict): The amount of resources to move.
        source (dict, optional): The source dictionary. Defaults to None.
        to (dict, optional): The target dictionary. Defaults to None.

    Returns:
        possible (bool): True if the move was possible, False otherwise.
    """
    if to is None:
        to = {}
    if source is None:
        source = {}
    for k, v in amount.items():
        if not k in to:
            to[k] = 0
        to[k] += v
        if source is None:
            continue
        if not k in source:
            source[k] = 0
        if source[k] < v:
            return False
        source[k] -= v

    return True

class ResourcePool:
    """A resource pool is a dictionary of resources that can be reserved. You can think of it has a multi-semaphore.
    """
    def __init__(self, directory, name, parent = NoParent):
        """Initialize the resource pool.

        Args:
            directory (str, callable): directory where the resource pool will be stored. If callable, it will be called to get the directory.
            name (str): name of the resource pool.
            parent (Lock, optional): parent lock. Defaults to NoParent.
        """        
        self.directory = directory
        self.name = name
        self.lock = Lock(directory, name, parent=parent)
    
    def get_file(self, name):
        """Get a file from the resource pool. The file could represent the resources of a run or the available resources.

        Args:
            name (str): name of the file.

        Returns:
            file (File): the file.
        """
        directory = self.directory
        if callable(directory):
            directory = directory()
        return File(os.path.join(directory, self.name + "_resource_pool"),name,binary=False, default = {}, load_func = json.loads, dump_func = json.dumps)
    
    def available_resources(self):
        """Get the available (unreserved) resources.

        Returns:
            file (File): the file with the available resources.
        """
        return self.get_file("available")
    
    def reserved_resources(self, run_id):
        """Get the reserved resources for a run.

        Args:
            run_id (str): id of the run.

        Returns:
            file (File): the file with the reserved resources.
        """
        return self.get_file(run_id)
    
    def move(self, amount, source = None, to = None):
        """Atomically move resources from one dictionary to another. Blocks until the move is possible.

        Args:
            amount (File): The amount of resources to move.
            source (File, dict, optional): Where the resources originate from. Defaults to None.
            to (File, optional): Where the resources go. Defaults to None.
        """
        
        while(True):
            with self.lock:
                source_vals = None
                to_vals = None
                if source:
                    source_vals = source.get()
                if to:
                    to_vals = to.get()
                if type(amount) is File:
                    amount_vals = amount.get()
                else:
                    amount_vals = amount

                if move(
                    amount = amount_vals, 
                    source = source_vals, 
                    to = to_vals
                    ):
                    if source:
                        source.set(source_vals)
                    if to:
                        to.set(to_vals)
                    break

            time.sleep(CHECK_INTERVAL)


    def reserve_resources(self, run_id, **resources):
        """Reserve resources for a run.

        Args:
            run_id (str): id of the run.
        """
        
        return self.move(
            amount = resources,
            source = self.available_resources(),
            to = self.reserved_resources(run_id=run_id)
        )
    
    def free_global_resources(self, **resources):
        """Free resources for all runs.
        """
        return self.move(
            amount = resources,
            to = self.available_resources()
        )
    
    def free_resources(self,run_id, **resources):
        """Free resources for a run.

        Args:
            run_id (str): id of the run.
        """        """"""
        return self.move(
            amount = resources,
            source = self.reserved_resources(run_id),
            to = self.available_resources()
        )

    def free_reserved_resources(self, run_id):
        """Free all reserved resources for a run.

        Args:
            run_id (str): id of the run.
        """
        return self.move(
            amount = self.reserved_resources(run_id),
            source = self.reserved_resources(run_id),
            to = self.available_resources()
        )