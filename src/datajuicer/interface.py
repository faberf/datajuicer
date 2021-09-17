

class Frame(list):
    @staticmethod
    def new():
        return Frame([{}])

def configure(frame, configuration):
    raise NotImplementedError

def vary(frame, key, values):
    raise NotImplementedError

def where(frame, condition):
    raise NotImplementedError

def remove_duplicates(frame):
    raise NotImplementedError

class Runner:
    def __init__(self, n_threads=1, incognito=False) -> None:
        self.n_threads = n_threads
        self.incognito = incognito
    
    def run(frame, func, *args, **kwargs):
        raise NotImplementedError

def run(frame, func, *args, **kwargs):
    runner = Runner()
    return runner.run(frame, func, *args, **kwargs)

def run_parallel(n_threads, frame, func, *args, **kwargs):
    raise NotImplementedError

def select(key):
    raise NotImplementedError

def get_runs(frame, func, *args, **kwargs):
    raise NotImplementedError