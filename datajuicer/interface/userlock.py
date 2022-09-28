

from datajuicer.interface.states import get_cache, worker_semaphore


class Lock():
    def __init__(self, name):
        self.lock = get_cache().get_lock(f"user_lock_{name}")
    
    def __enter__(self):
        ws = worker_semaphore.get()
        ws.release()
        self.lock.__enter__()
        ws.acquire()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.lock.__exit__(exc_type, exc_val, exc_tb)