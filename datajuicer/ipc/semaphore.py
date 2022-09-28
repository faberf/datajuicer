

import time
from datajuicer.ipc.constants import CHECK_INTERVAL
from datajuicer.ipc.file import File, NoData
from datajuicer.ipc.lock import Lock


class Semaphore:
    def __init__(self, directory, name):
        self.directory = directory
        self.name = name
        self.lock = Lock(self.directory, self.name)
        self.file = File(self.directory, self.name)
    
    def acquire(self):
        while(True):
            with self.lock:
                data = self.file.get()
                if data is NoData:
                    val = 0
                else:
                    val = int(data)
                if val > 0:
                    val = val-1
                    self.file.set(str(val))
                    break
            time.sleep(CHECK_INTERVAL)
    
    def release(self):
        with self.lock:
            data = self.file.get()
            if data is NoData:
                val = 0
            else:
                val = int(data)
            self.file.set(str(val+1))
                
    def __enter__(self):
        return self.acquire()


    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.release()
