import os
import shutil
import ujson as json
import time
from datajuicer.lock import ILock


def make_dir(path):
    directory = os.path.dirname(path)
    if not os.path.isdir(directory):
        os.makedirs(directory)

class BaseCache:

    def all_runs(self):
        pass
    
    def get_start_time(self, run_id):
        pass

    def new_run(self, run_id):
        pass


    def conditional_new_run(self, run_id, rids_hash):
        pass

    def delete(self, run_id):
        pass

    def open(self, run_id, path, mode):
        pass

    def exists(self, run_id, path=None):
        pass
    
    def __enter__(self):
        pass

    def __exit__(self, *args, **kwargs):
        pass


class SimpleCache(BaseCache):

    def __init__(self, dir_path):
        if not os.path.isdir(dir_path):
            os.makedirs(dir_path)
        self.dir_path = dir_path
    
    def get_lock(self):
        return ILock("cache_lock", self.dir_path)
    
    def _all_runs(self):
        all_runs =  [f for f in os.listdir(self.dir_path) if self.exists(f)]
        return tuple(sorted(all_runs, key = lambda rid: -self._get_start_time(rid)))
    
    def all_runs(self):
        return self._all_runs()
    
    def _get_start_time(self, run_id):
        with self._open(run_id, "start_time.json", "r") as f:
            return json.load(f)
    
    def get_start_time(self, run_id):
        with self.get_lock():
            return self._get_start_time(run_id)
    
    
    def _new_run(self, run_id):
        os.makedirs(os.path.join(self.dir_path, run_id))
        with self._open(run_id, "start_time.json", "w+") as f:
            json.dump(time.time(), f)
    
    def new_run(self, run_id):
        with self.get_lock():
            return self._new_run(run_id)
    
    def _conditional_new_run(self, run_id, rids_hash):
        if hash(self._all_runs()) == rids_hash:
            self._new_run(run_id)
            return True
        return False

    def conditional_new_run(self, run_id, rids_hash):
        with self.get_lock():
            return self._conditional_new_run(run_id, rids_hash)

    def _open(self, run_id, path, mode):
        fullpath = os.path.join(self.dir_path, run_id, path)
        make_dir(fullpath)
        return open(fullpath, mode) 
    
    def open(self, run_id, path, mode):
        return self._open(run_id, path, mode)
    
    def _delete(self, run_id):
        shutil.rmtree(os.path.join(self.dir_path, run_id))
    
    def delete(self,run_id):
        with self.get_lock():
            self._delete(run_id)
    
    def exists(self, run_id, path=None):
        full_path = os.path.join(self.dir_path, run_id)
        if not os.path.isdir(full_path) or not os.path.exists(os.path.join(full_path, "start_time.json")):
            return False
        if not path is None:
            full_path = os.path.join(full_path, path)
        return os.path.exists(full_path)