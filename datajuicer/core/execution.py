


import time
from datajuicer.core.active import Context, restore, snapshot
from datajuicer.core.redirect import Redirect
from datajuicer.core.run import Run
from datajuicer.ipc.file import File
from datajuicer.utils import Ticker, make_id
import dill as pickle
import datajuicer.core.launch as launch


class Execution(Run):
    """ This class is a wrapper around Run that can be used to execute it. As opposed to the run class it contains the uncompressed parameters. It also contains the necessary context in order to synchronize with other executions.
    """
    def __init__(
        self,         
        cache,
        function,  #TODO: is this function a vanilla function or an IPC function?
        params,
        worker_semaphore, 
        resource_pool, 
        tmp_directory,
        ):
        super().__init__(make_id(), cache, function)
        self.params = params
        self.worker_semaphore = worker_semaphore
        self.resource_pool = resource_pool
        self.tmp_directory = tmp_directory
    
    def launch(self, launcher, last_hash = None):
        self.worker_semaphore.release()
        self.cache.insert({"id":self.run_id, "func":self.function, "start_time":time.time(), "params":self.params},last_hash=last_hash)
        self.record_pending(launcher.pending_cooldown)
        self.alive_cooldown = launcher.alive_cooldown
        self.tick_every = launcher.tick_every
        self.snapshot = snapshot()
        launcher.launch(self)
        self.worker_semaphore.acquire()
    
    def execute(self):
    
        ticker = Ticker(lambda : self.record_alive(self.alive_cooldown), self.tick_every)
        ticker.start()

        restore(self.snapshot)
        with Context(execution = self):
            with self.worker_semaphore:
                with Redirect(self.open("log.txt", "w+")):
                    try:
                        res = self.function(**self.params)
                    except Exception as e:
                        self.record_exception(e)
                        raise e
                self.resource_pool.free_reserved_resources(self.run_id)
                
                ticker.stop()
                self.record_complete(res)
        

        
    def make_job(self):
        file = File(directory=self.tmp_directory, name = f"{self.run_id}.pickle", binary=True, dump_func=pickle.dumps)
        file.set(self)
        path = file.get_file_path()

        entry = launch.__file__
        return f"python {entry} -path {path}"