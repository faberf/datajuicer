

from contextlib import nullcontext
import os

from datajuicer.utils import make_dir


class NoLock:
    pass


class NoData:
    pass

class File:
    def __init__(self, directory, name, binary=False, default=NoData, lock=NoLock, load_func = lambda x: x, dump_func=lambda x: x):
        self.directory = directory
        self.name = name
        self.default = default
        self.load_func = load_func
        self.dump_func = dump_func
        if lock is NoLock:
            lock = nullcontext()
        self.lock = lock
        if binary:
            self.read_mode = "br"
            self.write_mode = "bw+"
        else:
            self.read_mode = "r"
            self.write_mode = "w+"
    
    def get_file_path(self):
        directory = self.directory
        if callable(directory):
            directory = directory()
        return os.path.join(directory, self.name)
    
    def set(self, data):
        fp = self.get_file_path()
        make_dir(fp)
        with self.lock:
            with open(fp, self.write_mode) as f:
                f.write(self.dump_func(data))
                f.flush()
                os.fsync(f.fileno())

    def get(self):
        with self.lock:
            if not os.path.isfile(self.get_file_path()):
                return self.default
            with open(self.get_file_path(), self.read_mode) as f:
                return self.load_func(f.read())