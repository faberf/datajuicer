import os
import time
import ujson as json
from datajuicer.ipc.constants import CHECK_INTERVAL

from datajuicer.ipc.file import File
from datajuicer.ipc.lock import Lock, NoParent




def move(amount,source=None, to=None):

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
    def __init__(self, directory, name, parent = NoParent):
        self.directory = directory
        self.name = name
        self.lock = Lock(directory, name, parent=parent)
    
    def get_file(self, name):
        directory = self.directory
        if callable(directory):
            directory = directory()
        return File(os.path.join(directory, self.name + "_resource_pool"),name,binary=False, default = {}, load_func = json.loads, dump_func = json.dumps)
    
    def available_resources(self):
        return self.get_file("available")
    
    def reserved_resources(self, run_id):
        return self.get_file(run_id)
    
    def move(self, amount, source = None, to = None):
        while(True):
            with self.lock:
                source_vals = source.get()
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
                    source.set(source_vals)
                    to.set(to_vals)
                    break

            time.sleep(CHECK_INTERVAL)


    def reserve_resources(self, run_id, **resources):
        return self.move(
            amount = resources,
            source = self.available_resources(),
            to = self.reserved_resources()
        )
    
    def free_global_resources(self, **resources):
        return self.move(
            amount = resources,
            to = self.available_resources()
        )
    
    def free_resources(self,run_id, **resources):
        return self.move(
            amount = resources,
            source = self.reserved_resources(run_id),
            to = self.available_resources()
        )

    def free_reserved_resources(self, run_id):
        return self.move(
            amount = self.reserved_resources(run_id),
            source = self.reserved_resources(run_id),
            to = self.available_resources()
        )