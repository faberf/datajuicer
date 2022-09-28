import collections
import dill as pickle

def to_doc(obj):
    t = type(obj)
    if issubclass(t, Document):
        return obj
    if t is dict:
        return DictDocument(obj)
    if t is list:
        return ListDocument(obj)
    if t is int or t is str or t is float:
        return Document(obj)
    if callable(obj) and hasattr(obj, "__module__") and hasattr(obj, "__name__"):
        return CallableDocument(obj)
    if obj is None:
        return Document(obj)
    if isinstance(obj, collections.Hashable):
        return HashableDocument(obj)
    return UnknownDocument(obj)


class Document:
    def __init__(self, obj):
        self.__doc_obj = obj
    
    def extract(self):
        return self.__doc_obj

class CompressedDocument(Document):
    def __init__(self, obj):
        super().__init__(obj)
    
    def extract(self):
        return self
    
    def __eq__(self, other):
        if not type(self) is type(other):
            raise TypeError
        
        return self.obj == other.obj

def iscompressed(doc):
    doc = to_doc(doc)
    return issubclass(type(doc), CompressedDocument)

class CallableDocument(CompressedDocument):
    def __init__(self, func):
        super().__init__((func.__module__, func.__name__))

class HashableDocument(CompressedDocument):
    def __init__(self, obj):
        super().__init__(hash(obj))

class UnknownDocument(CompressedDocument):
    def __init__(self, obj):
        super().__init__(hash(pickle.dumps(obj)))

class DictDocument(Document):
    def __init__(self, fields):
        self.fields = {key:to_doc(val) for key,val in fields.items()}
    
    def extract(self, *path):
        if len(path) == 0:
            return {key:val.extract() for key,val in self.fields.items()}
        return self.fields[path[0]].extract(path[1:])
    

class ListDocument(Document):
    def __init__(self, items):
        self.items = [to_doc(item) for item in items]
    
    def extract(self, *path):
        if not type(path[0]) is int:
            raise TypeError
        if len(path) == 0:
            return [item.extract() for item in self.items]
        return self.fields[path[0]].extract(path[1:])


