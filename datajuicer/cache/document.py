import collections
import dill as pickle

from datajuicer.ipc.function import Function

def to_doc(obj):
    t = type(obj)
    if issubclass(t, Document):
        return obj
    if t is dict:
        return {key: to_doc(value) for key,value in obj.items()}
    if t is list:
        return [to_doc(item) for item in obj]
    if t is int or t is str or t is float or obj is None:
        return obj
    if callable(obj) and hasattr(obj, "__module__") and hasattr(obj, "__name__"):
        if not '<locals>' in obj.__qualname__:
            return obj
        return CallableDocument(obj)
    if isinstance(obj, collections.Hashable):
        return obj#HashableDocument(obj)
    return UnknownDocument(obj)


# class Document:
#     def __init__(self, obj):
#         self.obj = obj
    
#     def extract(self):
#         return self.obj

# class StatefulDocument:
#     def __init__(self, obj):
#         self.state = obj.__getstate__()
#         self.
    

class Document:
    def __init__(self, obj, t):
        self.objtype = t
        self.obj = obj
    
    def __getstate__(self):
        return {"obj": pickle.dumps(self.obj), "objtype": pickle.dumps(self.objtype)}

    def __setstate__(self, state):
        self.obj = pickle.loads(state["obj"])
        self.objtype = pickle.loads(state["objtype"])
    
    def __eq__(self, other):
        otherdoc = to_doc(other)
        if not type(self) is type(otherdoc):
            return False
        if not self.objtype is otherdoc.objtype:
            return False
        
        return self.obj == otherdoc.obj

    @classmethod 
    def load(cls, obj):
        self = cls.__new__(cls)
        self.obj = obj
        return self


class CallableDocument(Document):
    def __init__(self, func):
        super().__init__((func.__module__, func.__name__), type(func))

# class HashableDocument(Document):
#     def __init__(self, obj):
#         super().__init__(hash(obj), type(obj))

class UnknownDocument(Document):
    def __init__(self, obj):
        super().__init__(hash(pickle.dumps(obj)), type(obj))

# class DictDocument(Document):
#     def __init__(self, fields):
#         self.fields = {key:to_doc(val) for key,val in fields.items()}
    
#     def extract(self, *path):
#         if len(path) == 0:
#             return {key:val.extract() for key,val in self.fields.items()}
#         return self.fields[path[0]].extract(path[1:])
    

# class ListDocument(Document):
#     def __init__(self, items):
#         self.items = [to_doc(item) for item in items]
    
#     def extract(self, *path):
#         if len(path) == 0:
#             return [item.extract() for item in self.items]
#         if not type(path[0]) is int:
#             raise TypeError
#         return self.items[path[0]].extract(path[1:])


