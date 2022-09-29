from datajuicer.cache.document import DictDocument, ListDocument, to_doc
from datajuicer.cache.query import NoDocument, Query


def list_get (l, idx, default):
    try:
        return l[idx]
    except IndexError:
        return default

class _Equal(Query):
    def __init__(self, obj, strict):
        self.obj = obj
        self.strict = strict
    
    def extract(self):
        if type(self.obj) is list:
            return [item.extract() for item in self.obj]
        
        if type(self.obj) is dict:
            return {key: val.extract() for key, val in self.obj}
        
        return self.obj
    
    def check(self, document):
        if document is NoDocument:
            return False
        
        document = to_doc(document)
        if type(self.obj) is dict:
            if not type(document) is DictDocument:
                return False
            
            if self.strict and not set(self.obj) == set(document.fields):
                return False
            
            for key, val in self.obj.items():
                if not val.check(document.fields.get(key, NoDocument)):
                    return False
            
            return True
            
        if type(self.obj) is list:
            if not type(document) is ListDocument:
                return False
            
            if self.strict and not len(self.obj) == len(document.items):
                return False
            
            for i, item in enumerate(self.obj):
                if not item.check(list_get(document.items, i, NoDocument)):
                    return False

            return True
        
        return to_doc(self.obj).extract() == document.extract()

class Matches(_Equal):
    def __init__(self, obj):
        super().__init__(obj, strict = False)

class Exactly(_Equal):
    def __init__(self, obj):
        super().__init__(obj, strict = True)

def make_query(obj):
    if isinstance(obj, Query):
        return obj
    if type(obj) is dict:
        return Exactly({key:make_query(val) for key,val in obj.items()})
    if type(obj) is list:
        return Exactly([make_query(item) for item in obj])
    
    return Exactly(obj)
