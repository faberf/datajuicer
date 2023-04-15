

from contextlib import nullcontext
import os

from datajuicer.utils import make_dir


class NoLock:
    pass


class NoData:
    """A class that represents no data."""
    pass

class File:
    """An abstraction for a file that can be read and written to.
    """
    def __init__(self, directory, name, binary=False, default=NoData, lock=NoLock, load_func = lambda x: x, dump_func=lambda x: x):
        """

        Args:
            directory (str or callable): Directory where the file is located.
            name (str): Name of the file.
            binary (bool, optional): Whether the file is binary. Defaults to False.
            default (object, optional): The value of the file if it does not exist. Defaults to NoData.
            lock (Lock, optional): The lock to use when reading and writing to the file. Defaults to NoLock.
            load_func (callable, optional): The post-processing function to use when reading from the file. Defaults to lambda x: x.
            dump_func (callable, optional): The pre-processing function to use when writing to the file. Defaults to lambda x: x.
        """
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
        """Get the path to the file.

        Returns:
            path (str): The path to the file.
        """
        directory = self.directory
        if callable(directory):
            directory = directory()
        return os.path.join(directory, self.name)
    
    def set(self, data):
        """Set the data of the file.

        Args:
            data (object): The data to set.
        """        
        fp = self.get_file_path()
        make_dir(fp)
        with self.lock:
            with open(fp, self.write_mode) as f:
                f.write(self.dump_func(data))
                f.flush()
                os.fsync(f.fileno())

    def get(self):
        """Get the data of the file.

        Returns:
            data (object): The data of the file.
        """        
        with self.lock:
            if not os.path.isfile(self.get_file_path()):
                return self.default
            with open(self.get_file_path(), self.read_mode) as f:
                return self.load_func(f.read())