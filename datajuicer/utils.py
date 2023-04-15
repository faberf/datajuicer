import inspect
import os
import string
import random
import threading
import time

ID_LEN = 6

def make_id():
    """Make a random id. Does not affect the random state.

    Returns:
        str: The random id.
    """
    state = random.getstate()
    random.seed()
    letters = string.ascii_letters + string.digits
    ret = ''.join(random.choice(letters) for i in range(ID_LEN))

    random.setstate(state)
    return ret


def string_to_int(string):
    """Convert a string to an integer.

    Args:
        string (str): The string to convert.

    Returns:
        out (int): The integer.
    """
    bytes = string.encode("utf-8")
    return int.from_bytes(bytes, byteorder="big")

def int_to_string(integer):
    """Convert an integer to a string.

    Args:
        integer (int): The integer to convert.

    Returns:
        out (str): The string.
    """
    bytes = integer.to_bytes(((integer.bit_length() + 7) // 8), byteorder="big")
    return bytes.decode("utf-8")


def make_dir(path):
    """Make a directory if it does not exist.

    Args:
        path (str): The path to the directory.
    """ 
    directory = os.path.dirname(path)
    if not os.path.isdir(directory):
        try:
            os.makedirs(directory)
        except FileExistsError:
            pass

class Ticker(threading.Thread):
    """A thread that calls a function every tick.
    """
    def __init__(self, tick_func, tick_every):
        """Initialize the Ticker.

        Args:
            tick_func (callable): The function to call every tick.
            tick_every (float): the time between ticks in seconds.
        """
        super().__init__()
        self.parent = threading.current_thread()
        self.tick_every = tick_every
        self.tick_func = tick_func
        self.stopped = False
    
    def stop(self):
        """Stop the ticker.
        """
        self.stopped = True
    
    def run(self):

        while self.parent.is_alive() and not self.stopped:
            self.tick_func()
            time.sleep(self.tick_every)

def apply_defaults(func, *args, **kwargs):
    """Augment the arguments of a function with the default values and return them as a dictionary.

    Args:
        func (callable): The function to get the default values from.

    Returns:
        parameters (dict): The parameters of the function with the default values.
    """
    sig = inspect.signature(func)

    boundargs = sig.bind(*args,**kwargs)
    boundargs.apply_defaults()

    return dict(boundargs.arguments)