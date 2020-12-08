class Defer:
    def __init__(self,function):
        self.function = function
    
    def run(self, grid, n_threads):
        pass


class _special:
    pass

class ALL(_special):

    @classmethod
    def get(grid, data):
        return data

class GRID(_special):

    @classmethod
    def get(grid, data):
        return grid

class KEY(_special):
    def __init__(self, key):
        self.key = key

    def get(self,grid, data):
        ret = self.key
        while (isintance(ret, _special) or issubclass(ret, _special)) and get in vars(ret):


class FORMAT(_special):
    pass