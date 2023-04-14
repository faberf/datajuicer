

from datajuicer.interface.states import get_cache, worker_semaphore


class Lock():
    """This class is used to create a lock that is shared across all sessions. This is useful for when you want to make sure that only one session is accessing a resource at a time, for example when downloading a large file from the internet that is used across all sessions. This class is a context manager, so you can use it with the `with` statement. When you exit the `with` statement, you will release the lock. 
    """
    def __init__(self, name):
        self.lock = get_cache().get_lock(f"user_lock_{name}")
    
    def __enter__(self):
        ws = worker_semaphore.get()
        ws.release()
        self.lock.__enter__()
        ws.acquire()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.lock.__exit__(exc_type, exc_val, exc_tb)