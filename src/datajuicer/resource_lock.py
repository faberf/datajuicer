import threading
import datajuicer as dj

class ResourceLock:
    def __init__(self, max_n_threads = 1, **max_resources):
        self.threads = {}
        self.max_resources = max_resources
        self.lock = threading.RLock()
        self.used_resources = {}
        self.available_cond = threading.Condition(self.lock)
        self.n_thread_semaphore = threading.Semaphore(max_n_threads)
    
    def available(self, **resources):
        all_used_resources = {}
        for d in self.used_resources.values():
            for key, val in d.items():
                if not key in all_used_resources:
                    all_used_resources[key] = 0
                all_used_resources[key] += val
        return all([all_used_resources[key] > val  for key, val in resources.items()])
    
    def reserve_resources(self, **resources):
        for val in resources.values():
             if val<0:
                 raise TypeError
        rid = dj.run_id()
        self.release()
        with self.available_cond:
            while not self.available(**resources):
                self.available_cond.wait()
            if not rid in self.used_resources:
                self.used_resources[rid] = {}
            for key, val in resources.items():
                if not key in self.used_resources[rid]:
                    self.used_resources[rid][key] = 0.0
                self.used_resources[rid][key] += val
        self.acquire()
    
    def free_resources(self, **resources):
        for val in resources.values():
             if val<0:
                 raise TypeError
        rid = dj.run_id()
        self.release()
        if rid in self.used_resources:
            with self.available_cond:
                for key, val in resources.items():
                    if key in self.used_resources[rid]:
                        if self.used_resources[rid][key] > val:
                            self.used_resources[rid][key] -= val
                self.available_cond.notify_all()
        self.acquire()
    
    def free_all_resources(self):
        rid = dj.run_id()
        self.release()
        with self.available_cond:
            del self.used_resources[rid]
            self.available_cond.notify_all()

        self.n_thread_semaphore.acquire()
    
    def acquire(self):
        self.n_thread_semaphore.acquire()
    
    def release(self):
        self.n_thread_semaphore.release()