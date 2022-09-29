import os
from datajuicer.lock import ILock, FileData, sem_from_lock_and_data


class ScratchSpace:

    def __init__(self, directory):
        if not os.path.isdir(directory):
            os.makedirs(directory)
        self.directory = directory
    
    def get_lock(self, lock_name):
        return ILock(lock_name, self.directory)

    def get_semaphore(self, semaphore_name):
        return sem_from_lock_and_data(ILock, FileData)(semaphore_name, self.directory)

    def get_file_data(self, file_name, binary=False):
        return FileData(file_name, self.directory, binary)
    