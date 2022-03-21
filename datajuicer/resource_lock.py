from sqlite3.dbapi2 import OperationalError
import threading
import json
import os
import time
import posix_ipc
import mmap
import datajuicer
import copy
import datajuicer.ilock as ilock


# class PosixSemaphore:
#     def __init__(self, name, init, directory):
#         if init:
#             flag = posix_ipc.O_CREX
#             try:
#                 posix_ipc.unlink_semaphore(name)
#             except Exception:
#                 pass
        
#         else:
#             flag = 0
#         self.sem = posix_ipc.Semaphore(name, flag)
#     def acquire(self):
#         return self.sem.acquire()
#     def release(self):
#         return self.sem.release()

#     def __enter__(self):
#         return self.sem.__enter__()
#     def __exit__(self ,*args,**kwargs):
#         return self.sem.__exit__(*args, **kwargs)


def lock_from_sem(sem_cls):
    class Lock(sem_cls):
            def __init__(self, name, init, directory):
                super().__init__(name, init, directory)
                if init:
                    self.release()
    return Lock

def sem_from_lock_and_data(lock_cls, data_cls):

    class Semaphore:
        def __init__(self, name, init, directory):
            self.lock = lock_cls(name + "_lock", init, directory)
            self.data = data_cls(name + "_data", init, directory)
            if init:
                self.data.set(json.dumps(0))
        
        def acquire(self):
            while(True):
                with self.lock:
                    data = self.data.get()
                    val = json.loads(data)
                    if val > 0:
                        val = val-1
                        self.data.set(json.dumps(val))
                        break
                time.sleep(0.1)
        def release(self):
            with self.lock:
                val = json.loads(self.data.get())
                self.data.set(json.dumps(val+1))
        
    return Semaphore

        


# class PosixData:
#     size = 100
#     def __init__(self, name, init, directory):
#         if init:
#             flag = posix_ipc.O_CREX
#             try:
#                 posix_ipc.unlink_shared_memory(name)
#             except Exception:
#                 pass
        
#         else:
#             flag = 0
#         memory = posix_ipc.SharedMemory(name, flag, size=PosixData.size)
#         self.mapfile = mmap.mmap(memory.fd, memory.size)

#         memory.close_fd()
    
#     def set(self, string):
#         self.mapfile.seek(0)
#         self.mapfile.write(string.encode()+ b"\0")
    
#     def get(self):
#         self.mapfile.seek(0)
#         s = []
#         c = self.mapfile.read_byte()
#         while c != 0:
#             s.append(c)
#             c = self.mapfile.read_byte()
#         s = [chr(c) for c in s]
#         return ''.join(s)

class ILock(ilock.ILock):
    def __init__(self, name, init, directory):
        super().__init__(name)
class FileData:
    def __init__(self, name, init, directory):
        self.path = os.path.join(directory, f"{name}_data.txt")
    
    def set(self, string):
        with open(self.path, "w+") as f:
            f.write(string)

    def get(self):
        with open(self.path, "r") as f:
            return f.read()



class ResourceLock:

    def __init__(self, session, directory = "dj_resources/", init=False, semaphore=sem_from_lock_and_data(ILock, FileData), data=FileData, lock=ILock):
        self.session = session
        self.directory = directory
        datajuicer.cache.make_dir(directory)
        self.workers_semaphore = semaphore(f"djworkers_{session}", init, directory)
        self.resources = data(f"djresources_{session}", init, directory)
        self.lock = lock(f"djlock_{session}", init, directory)
        if init:
            with self.lock:
                self.resources.set(json.dumps({}))
    
    def available(self):
        return json.loads(self.resources.get())
    
    def is_available(self, available, resources):
        for val in resources.values():
            if val < 0:
                raise TypeError
        for k, v in resources.items():
            if not k in available:
                return False
            if available[k] < v:
                return False
        return True
    
    def reserve_resources(self, **resources):
        c = datajuicer.launch._get_context()
        uid = c.context_id
        local_res_path = os.path.join(self.directory, f"{uid}_resources.json")
        if os.path.exists(local_res_path):
            with open(local_res_path, "r") as f:
                local_resources = json.load(f)
        else:
            local_resources = {}
        
        self.workers_semaphore.release()
        while(True):

            with self.lock:
                av = self.available()
                if self.is_available(av, resources):
                    av_old = copy.copy(av)
                    
                    for k, v in resources.items():
                        if not k in local_resources:
                            local_resources[k] = 0
                        av[k] -= v
                        local_resources[k] += v
                    with open(local_res_path, "w+") as f:
                        f.truncate(0)
                        json.dump(local_resources, f)

                    assert(self.available() == av_old)
                    self.resources.set(json.dumps(av))
                    #print(f"Available resources updated from {av_old} to {av} in session {self.session}")
                    
                    break

            time.sleep(0.1)

        self.workers_semaphore.acquire()
    
    def free_resources(self, **resources):
        c = datajuicer.launch._get_context()
        if c is not None:
            uid = c.context_id
            local_res_path = os.path.join(self.directory, f"{uid}_resources.json")
            if os.path.exists(local_res_path):
                with open(local_res_path, "r") as f:
                    local_resources = json.load(f)
            else:
                local_resources = {}
            
        self.workers_semaphore.release()
        with self.lock:
            av = self.available()
            av_old = copy.copy(av)
            for k,v in resources.items():
                if c is not None:
                    if not k in local_resources:
                        local_resources[k] = 0
                    local_resources[k] -= v
                if not k in av:
                    av[k] = 0
                av[k] += v 
            if c is not None:
                with open(local_res_path, "w+") as f:
                    f.truncate(0)
                    json.dump(local_resources, f)
        
            self.resources.set(json.dumps(av))
            #print(f"Available resources updated from {av_old} to {av} in session {self.session}")
        self.workers_semaphore.acquire()

    def free_all_resources(self):
        uid = datajuicer.launch._get_context().context_id
        local_res_path = os.path.join(self.directory, f"{uid}_resources.json")
        if os.path.exists(local_res_path):
            with open(local_res_path, "r") as f:
                local_resources = json.load(f)
            self.free_resources(**local_resources)

    def acquire(self):
        self.workers_semaphore.acquire()
        #print(f"{self.session} {self.workers_semaphore.value} acq")


    def release(self):
        self.workers_semaphore.release()
        #print(f"{self.session} {self.workers_semaphore.value} rel")

# class ResourceLock:
#     def __init__(self, session, directory = "dj_resources/", init=False):

#         self.directory = directory
#         datajuicer.cache.make_dir(directory)
#         if init:
#             try:
#                 posix_ipc.unlink_semaphore(f"djworkers_{session}")
#             except Exception:
#                 pass
#             try:
#                 posix_ipc.unlink_shared_memory(f"djresources_{session}")
#             except Exception:
#                 pass
#             try:
#                 posix_ipc.unlink_semaphore(f"djlock_{session}")
#             except Exception:
#                 pass
#         if init:
#             flag = posix_ipc.O_CREX
#         else:
#             flag = 0
#         self.workers_semaphore = posix_ipc.Semaphore(f"djworkers_{session}", flag)
#         self.lock = posix_ipc.Semaphore(f"djlock_{session}", flag)
        
#         memory = posix_ipc.SharedMemory(f"djresources_{session}", flag, size=100)
#         self.mapfile = mmap.mmap(memory.fd, memory.size)

#         memory.close_fd()
#         self.session = session
#         if init:
#             print(f"initializing session {self.session}, currently the lock is {self.lock.value}")
#             self.lock.release()
#             print(f"now {self.lock.value}")
#             with self.lock:
#                 self.mapfile.seek(0)
#                 self.mapfile.write(json.dumps({}).encode()+ b"\0")

#     def available(self):
#         self.mapfile.seek(0)
#         s = []
#         c = self.mapfile.read_byte()
#         while c != 0:
#             s.append(c)
#             c = self.mapfile.read_byte()
#         s = [chr(c) for c in s]
#         s = ''.join(s)
#         return json.loads(s)

    
#     def is_available(self, available, resources):
#         for val in resources.values():
#             if val < 0:
#                 raise TypeError
#         for k, v in resources.items():
#             if not k in available:
#                 return False
#             if available[k] < v:
#                 return False
#         return True
    
#     def reserve_resources(self, **resources):
#         c = datajuicer.launch._get_context()
#         uid = c.context_id
#         local_res_path = os.path.join(self.directory, f"{uid}_resources.json")
#         if os.path.exists(local_res_path):
#             with open(local_res_path, "r") as f:
#                 local_resources = json.load(f)
#         else:
#             local_resources = {}
        
#         self.workers_semaphore.release()
#         while(True):

#             with self.lock:
#                 av = self.available()
#                 if self.is_available(av, resources):
#                     av_old = copy.copy(av)
                    
#                     for k, v in resources.items():
#                         if not k in local_resources:
#                             local_resources[k] = 0
#                         av[k] -= v
#                         local_resources[k] += v
#                     with open(local_res_path, "w+") as f:
#                         f.truncate(0)
#                         json.dump(local_resources, f)

#                     assert(self.available() == av_old)
#                     self.mapfile.seek(0)
#                     self.mapfile.write(json.dumps(av).encode() + b"\0")
#                     #print(f"Available resources updated from {av_old} to {av} in session {self.session}")
                    
#                     break

#             time.sleep(0.1)

#         self.workers_semaphore.acquire()

#     def free_resources(self, **resources):
#         c = datajuicer.launch._get_context()
#         if c is not None:
#             uid = c.context_id
#             local_res_path = os.path.join(self.directory, f"{uid}_resources.json")
#             if os.path.exists(local_res_path):
#                 with open(local_res_path, "r") as f:
#                     local_resources = json.load(f)
#             else:
#                 local_resources = {}
            
#         self.workers_semaphore.release()
#         with self.lock:
#             av = self.available()
#             av_old = copy.copy(av)
#             for k,v in resources.items():
#                 if c is not None:
#                     if not k in local_resources:
#                         local_resources[k] = 0
#                     local_resources[k] -= v
#                 if not k in av:
#                     av[k] = 0
#                 av[k] += v 
#             if c is not None:
#                 with open(local_res_path, "w+") as f:
#                     f.truncate(0)
#                     json.dump(local_resources, f)
#             # cur.execute(f"UPDATE resources SET data = '{json.dumps(av)}'")
#             # conn.commit()
#             # conn.close()
        
#             self.mapfile.seek(0)
#             self.mapfile.write(json.dumps(av).encode()+ b"\0")
#             #print(f"Available resources updated from {av_old} to {av} in session {self.session}")
#         self.workers_semaphore.acquire()

#     def free_all_resources(self):
#         uid = datajuicer.launch._get_context().context_id
#         local_res_path = os.path.join(self.directory, f"{uid}_resources.json")
#         if os.path.exists(local_res_path):
#             with open(local_res_path, "r") as f:
#                 local_resources = json.load(f)
#             self.free_resources(**local_resources)

#     def acquire(self):
#         self.workers_semaphore.acquire()
#         print(f"{self.session} {self.workers_semaphore.value} acq")


#     def release(self):
#         self.workers_semaphore.release()
#         print(f"{self.session} {self.workers_semaphore.value} rel")


