import threading

from datajuicer.errors import AlreadySetException


"""
This module provides a way to store and retrieve global state in a thread-safe way.
"""

active = threading.local()

class Inactive:
    pass

def set_active(key, val):
    """Set the active value for a key.

    Args:
        key (str): the key
        val (obj): the value
    """
    if val is Inactive:
        return reset_active(key)
    setattr(active, key, val)

def reset_active(key):
    """Reset the active value for a key.

    Args:
        key (str): the key
    """
    delattr(active, key)

def get_active(key, default=Inactive):
    """Get the active value for a key.

    Args:
        key (str): the key
        default (object, optional): the default value if the key is not set. Defaults to Inactive.

    Returns:
        value (object): the value
    """
    if not hasattr(active, key):
        return default
    return getattr(active, key)

def snapshot():
    """Get a snapshot of the active state. This is useful for restoring the state later.

    Returns:
        snapshot (dict): the snapshot
    """
    return dict(active.__dict__)

def restore(snapshot):
    """Restore the active state from a snapshot.

    Args:
        snapshot (dict): the snapshot
    """
    for key in dict(active.__dict__):
        reset_active(key)
    for key,val in snapshot.items():
        set_active(key, val)

class Context:
    """A context manager for setting active state. This is useful for temporarily setting active state.
    """
    def __init__(self, **kwargs):
        self.state = kwargs
    
    def __enter__(self):
        self.old = {}
        for key,val in self.state.items():
            self.old[key] = get_active(key)
            set_active(key, val)

    def __exit__(self, exc_type, exc_val, exc_tb):
        for key,val in self.old.items():
            set_active(key, val)

class State:
    
    """An abstraction for managing global state. Provides a way to get and set the state, and a context manager for temporarily setting the state."""
    
    def __init__(self, key, default, mutable):
        self.key = key
        self.default = default
        self.mutable = mutable
    
    def get(self):
        ret = get_active(self.key)
        if not ret is Inactive:
            return ret
        
        set_active(self.key, self.default)
        return self.default
    
    def context(self, val):
        if not self.mutable:
            if not get_active(self.key) is Inactive:
                raise AlreadySetException
        
        return Context(**{self.key: val})
    

    def set(self, val):
        self.context(val).__enter__()