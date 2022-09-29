import json
import time
from datajuicer.lock import FileData


def is_available(available, required):
    for val in required.values():
        if val < 0:
            raise TypeError
    for k, v in required.items():
        if not k in available:
            return False
        if available[k] < v:
            return False
    return True



class ResourceLock:

    def __init__(self, session_id, scratch):
        self.scratch = scratch
        self.session_id = session_id
    
    def get_available_resources_filedata(self):
        return self.scratch.get_file_data(f"{self.session_id}_available_resources")
    
    def get_reserved_resources_filedata(self, run_id):
        return self.scratch.get_file_data(f"{self.session_id}_{run_id}_reserved_resources")
    
    def get_workers_semaphore(self):
        return self.scratch.get_semaphore(f"{self.session_id}_semaphore")
    
    def get_lock(self):
        return self.scratch.get_lock(f"{self.session_id}_lock")
    
    def available_resources(self):
        ret = self.get_available_resources_filedata().get()
        if ret is FileData.NoData:
            return {}
        return json.loads(ret)
    
    def reserved_resources(self, run_id):
        ret = self.get_reserved_resources_filedata(run_id).get()
        if ret is FileData.NoData:
            return {}
        return json.loads(ret)
    
    def reserve_resources(self, run_id, **resources):

        sem = self.get_workers_semaphore()
        available = self.available_resources()
        reserved = self.reserved_resources(run_id)
        
        sem.release()
        while(True):

            with self.get_lock():
                
                if is_available(available, resources):
                    
                    for k, v in resources.items():
                        if not k in reserved:
                            reserved[k] = 0
                        available[k] -= v
                        reserved[k] += v
                    self.get_reserved_resources_filedata(run_id).set(json.dumps(reserved))
                    self.get_available_resources_filedata().set(json.dumps(available))
                    
                    break

            time.sleep(0.1)

        sem.acquire()
    
    def free_global_resources(self, **resources):
        available = self.available_resources()
        
        with self.get_lock():
            for k,v in resources.items():
                if not k in available:
                    available[k] = 0
                available[k] += v 
            self.get_available_resources_filedata().set(json.dumps(available))
    
    def free_resources(self,run_id, **resources):
        available = self.available_resources()
        reserved = self.reserved_resources(run_id)
        
        with self.get_lock():
            for k,v in resources.items():
                if not k in reserved:
                    reserved[k] = 0
                reserved[k] -= v
                if not k in available:
                    available[k] = 0
                available[k] += v 
            self.get_reserved_resources_filedata(run_id).set(json.dumps(reserved))
            self.get_available_resources_filedata().set(json.dumps(available))

    def free_all_resources(self, run_id):
        available = self.available_resources()
        reserved = self.reserved_resources(run_id)
        
        with self.get_lock():
            for k,v in reserved.items():
                if not k in available:
                    available[k] = 0
                available[k] += v 
            self.get_reserved_resources_filedata(run_id).set(json.dumps({}))
            self.get_available_resources_filedata().set(json.dumps(available))
        

    def acquire(self):
        self.get_workers_semaphore().acquire()

    def release(self):
        self.get_workers_semaphore().release()

class UserLock:
    def __init__(self, name):
        from datajuicer.context import Context
        self.lock = Context.get_active().scratch.get_lock(f"user_lock_{name}")

    def __enter__(self):

        from datajuicer.context import Context
        rl = Context.get_active().resource_lock
        rl.release()
        self.lock.__enter__()
        rl.acquire()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.lock.__exit__(exc_type, exc_val, exc_tb)


