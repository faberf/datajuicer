

import time
from datajuicer.ipc.constants import CHECK_INTERVAL
from datajuicer.ipc.file import File, NoData
from datajuicer.ipc.lock import Lock, NoParent



class Semaphore:
    """Inter-process semaphore implemented using a file and a lock. This is a context manager, so you can use it with the `with` statement. When you exit the `with` statement, you will release the lock.
    """
    def __init__(self, directory, name, parent = NoParent):
        """Create a new semaphore.

        Args:
            directory (str, callable): directory where the semaphore file and the lock file is located.
            name (str): The name of the semaphore.
        """
        self.directory = directory
        self.name = name
        self.lock = Lock(self.directory, self.name)
        self.file = File(self.directory, self.name)
        self.parent = parent
    
    def acquire(self):
        """Acquire the semaphore. This will block until the semaphore is acquired or the timeout is reached.
        """
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
        """Release the semaphore. This will increase the value of the semaphore by 1.
        """
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
