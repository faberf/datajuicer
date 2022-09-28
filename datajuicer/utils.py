import inspect
import os
import string
import random
import threading
import time

ID_LEN = 6

def make_id():
    state = random.getstate()
    random.seed()
    letters = string.ascii_letters + string.digits
    ret = ''.join(random.choice(letters) for i in range(ID_LEN))

    random.setstate(state)
    return ret


def string_to_int(string):
    bytes = string.encode("utf-8")
    return int.from_bytes(bytes, byteorder="big")

def int_to_string(integer):
    bytes = integer.to_bytes(((integer.bit_length() + 7) // 8), byteorder="big")
    return bytes.decode("utf-8")


def make_dir(path):
    directory = os.path.dirname(path)
    if not os.path.isdir(directory):
        os.makedirs(directory)

class Ticker(threading.Thread):
    def __init__(self, tick_func, tick_every):
        super().__init__()
        self.parent = threading.current_thread()
        self.tick_every = tick_every
        self.tick_func = tick_func
    
    def run(self):

        while self.parent.is_alive():
            self.tick_func()
            time.sleep(self.tick_every)

def apply_defaults(func, *args, **kwargs):
    sig = inspect.signature(func)

    boundargs = sig.bind(*args,**kwargs)
    boundargs.apply_defaults()

    return dict(boundargs.arguments)