import time
import portalocker
from datajuicer.errors import ReenterException, TimeoutException
import os, threading
from datajuicer.ipc.constants import CHECK_INTERVAL, TIMEOUT

class NoParent:
    pass

state = threading.local()

class Lock:
    def __init__(self, directory, name, parent = NoParent):
        self.timeout = TIMEOUT if TIMEOUT is not None else 10 ** 8
        self.check_interval = CHECK_INTERVAL
        self.directory = directory
        self.name = name
        self.parent = parent
    
    def get_file_path(self):
        return os.path.join(self.directory, self.name+ "_lock")
    
    def acquire(self):
        if not self.parent is NoParent:
            self.parent.release()

        if not hasattr(state, "acquired"):
            state.acquired = False

        if state.acquired:
            raise ReenterException('Trying re-enter a non-reentrant lock')

        current_time = call_time =  time.time()
        while call_time + self.timeout >= current_time:
            state.lockfile = open(self.get_file_path(), 'w')
            try:
                portalocker.lock(state.lockfile, portalocker.constants.LOCK_NB | portalocker.constants.LOCK_EX)
                state.acquired = True
                
                if not self.parent is NoParent:
                    self.parent.acquire()
                return self
            except portalocker.exceptions.LockException:
                pass
            
            current_time = time.time()
            check_interval = CHECK_INTERVAL if TIMEOUT > CHECK_INTERVAL else TIMEOUT
            time.sleep(check_interval)

        raise TimeoutException('Timeout was reached')
    
    def release(self):
        state.lockfile.flush()
        os.fsync(state.lockfile.fileno())
        state.lockfile.close()
        state.acquired = False

    
    def __enter__(self):
        return self.acquire()


    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.release()

        