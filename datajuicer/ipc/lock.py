import time
import portalocker
from datajuicer.errors import ReenterException, TimeoutException
import os, threading
from datajuicer.ipc.constants import CHECK_INTERVAL, TIMEOUT
from datajuicer.utils import make_dir

class NoParent:
    pass

state = threading.local()
# state.acquired = set({})
# state.lockfiles = {}

# def acquire_file(fp):
#     if not hasattr(state, "acquired"):
#         state.acquired = set()
#         state.lockfiles = {}
#     if fp in state.acquired:
#         raise ReenterException('Trying re-enter a non-reentrant lock')
    
#     state.acquired.add(fp)
#     state.lockfiles[fp] = open(fp, 'w+')
#     return state.lockfiles[fp]

class Lock:
    """This is an multiprocessing safe lock using portalocker. Reentering is not supported. It is a context manager, so you can use it with the `with` statement. When you exit the `with` statement, you will release the lock.
    """
    def __init__(self, directory, name, parent = NoParent):
        """Create a new lock.

        Args:
            directory (str, callable): The directory where the lock file is located.
            name (str): The name of the lock file.
            parent (Lock, optional): The parent lock. This can be used to create a hierarchy of locks. The parent lock is released when the child lock is acquired. Defaults to NoParent.
        """        
        self.timeout = TIMEOUT if TIMEOUT is not None else 10 ** 8
        self.check_interval = CHECK_INTERVAL
        self.directory = directory
        self.name = name
        self.parent = parent
    
    def get_file_path(self):
        """Get the path to the lock file.

        Returns:
            str: The path to the lock file.
        """        
        directory = self.directory
        if callable(directory):
            directory = directory()
        return os.path.join(directory, self.name+ "_lock")
    
    def acquire(self):
        """Acquire the lock. This will block until the lock is acquired or the timeout is reached.

        Raises:
            ReenterException: Raised when trying to re-enter the lock.
            TimeoutException: Raised when the timeout is reached.

        Returns:
            self (Lock): The lock object.
        """        
        fp = self.get_file_path()
        if not self.parent is NoParent:
            self.parent.release()
        if not hasattr(state, "acquired"):
            state.acquired = set()
            state.lockfiles = {}
        if fp in state.acquired:
            raise ReenterException('Trying re-enter a non-reentrant lock')

        current_time = call_time =  time.time()
        while call_time + self.timeout >= current_time:
            
            make_dir(fp)
            state.lockfiles[fp] = open(fp, 'w+')
            try:
                portalocker.lock(state.lockfiles[fp], portalocker.constants.LOCK_NB | portalocker.constants.LOCK_EX)
                state.acquired.add(fp)
                
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
        """Release the lock.
        """
        fp = self.get_file_path()
        state.lockfiles[fp].flush()
        os.fsync(state.lockfiles[fp].fileno())
        state.lockfiles[fp].close()
        state.acquired.remove(fp)
        #TODO: acquire the parent lock??

    
    def __enter__(self):
        return self.acquire()


    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.release()

        