from sqlite3.dbapi2 import OperationalError
import threading
import json
import os
import time
import posix_ipc
import mmap
import datajuicer
import copy



class ResourceLock:
    def __init__(self, session, directory = "dj_resources/", init=False):

        self.directory = directory
        datajuicer.cache.make_dir(directory)
        if init:
            try:
                posix_ipc.unlink_semaphore(f"djworkers_{session}")
            except Exception:
                pass
            try:
                posix_ipc.unlink_shared_memory(f"djresources_{session}")
            except Exception:
                pass
            try:
                posix_ipc.unlink_semaphore(f"djlock_{session}")
            except Exception:
                pass
        self.workers_semaphore = posix_ipc.Semaphore(f"djworkers_{session}", posix_ipc.O_CREAT)
        self.lock = posix_ipc.Semaphore(f"djlock_{session}", posix_ipc.O_CREAT)
        
        memory = posix_ipc.SharedMemory(f"djresources_{session}", posix_ipc.O_CREAT, size=100)
        self.mapfile = mmap.mmap(memory.fd, memory.size)

        memory.close_fd()
        self.session = session
        if init:
            self.lock.release()
            with self.lock:
                self.mapfile.seek(0)
                self.mapfile.write(json.dumps({}).encode()+ b"\0")

    def available(self):
        self.mapfile.seek(0)
        s = []
        c = self.mapfile.read_byte()
        while c != 0:
            s.append(c)
            c = self.mapfile.read_byte()
        s = [chr(c) for c in s]
        s = ''.join(s)
        return json.loads(s)

    
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
                    self.mapfile.seek(0)
                    self.mapfile.write(json.dumps(av).encode() + b"\0")
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
            # cur.execute(f"UPDATE resources SET data = '{json.dumps(av)}'")
            # conn.commit()
            # conn.close()
        
            self.mapfile.seek(0)
            self.mapfile.write(json.dumps(av).encode()+ b"\0")
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


    def release(self):
        self.workers_semaphore.release()


