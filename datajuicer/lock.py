import json
import time
import threading
import portalocker
import os


def sem_from_lock_and_data(lock_cls, data_cls):

    class Semaphore:
        def __init__(self, name, directory):
            self.lock = lock_cls(name + "_lock", directory)
            self.data = data_cls(name + "_data", directory)
        
        def acquire(self):
            while(True):
                with self.lock:
                    data = self.data.get()
                    if data is FileData.NoData:
                        data = "0"
                    val = json.loads(data)
                    if val > 0:
                        val = val-1
                        self.data.set(json.dumps(val))
                        break
                time.sleep(0.1)
        def release(self):
            with self.lock:
                data = self.data.get()
                if data is FileData.NoData:
                    data = "0"
                val = json.loads(data)
                self.data.set(json.dumps(val+1))
        
    return Semaphore


class ILockException(Exception):
    pass

CHECK_INTERVAL = 0.25
TIMEOUT = None
REENTRANT = True

class ILock(object):
    def __init__(self, name, directory):
        self._timeout = TIMEOUT if TIMEOUT is not None else 10 ** 8
        self._check_interval = CHECK_INTERVAL

        self._filepath = os.path.join(directory, name)

        self._reentrant = True
        self.local = threading.local()
        self.local._enter_count = 0
        

    def __enter__(self):
        if not hasattr(self.local, "_enter_count"):
            self.local._enter_count = 0

        if self.local._enter_count > 0:
            if self._reentrant:
                self.local._enter_count += 1
                return self
            raise ILockException('Trying re-enter a non-reentrant lock')

        current_time = call_time =  time.time()
        while call_time + self._timeout >= current_time:
            self.local._lockfile = open(self._filepath, 'w')
            try:
                portalocker.lock(self.local._lockfile, portalocker.constants.LOCK_NB | portalocker.constants.LOCK_EX)
                self.local._enter_count = 1
                return self
            except portalocker.exceptions.LockException:
                pass

            current_time = time.time()
            check_interval = self._check_interval if self._timeout > self._check_interval else self._timeout
            time.sleep(check_interval)

        raise ILockException('Timeout was reached')

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.local._enter_count -= 1

        if self.local._enter_count > 0:
            return

        self.local._lockfile.close()



class FileData:
    class NoData:
        pass
    def __init__(self, name, directory, binary = False):
        self.path = os.path.join(directory, name)
        if binary:
            self.read_mode = "br"
            self.write_mode = "bw+"
        else:
            self.read_mode = "r"
            self.write_mode = "w+"
    
    def set(self, data):
        with open(self.path, self.write_mode) as f:
            f.write(data)

    def get(self):
        if not os.path.isfile(self.path):
            return FileData.NoData
        with open(self.path, self.read_mode) as f:
            return f.read()