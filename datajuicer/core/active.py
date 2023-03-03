import threading

from datajuicer.errors import AlreadySetException

active = threading.local()

class Inactive:
    pass

def set_active(key, val):
    if val is Inactive:
        return reset_active(key)
    setattr(active, key, val)

def reset_active(key):
    delattr(active, key)

def get_active(key, default=Inactive):
    if not hasattr(active, key):
        return default
    return getattr(active, key)

def snapshot():
    return dict(active.__dict__)

def restore(snapshot):
    for key in dict(active.__dict__):
        reset_active(key)
    for key,val in snapshot.items():
        set_active(key, val)

class Context:
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