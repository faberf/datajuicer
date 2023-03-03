from datajuicer.cache.document import to_doc
from datajuicer.cache.query import Query, NoDocument


class _Equal(Query):
    def __init__(self, obj, strict):
        self.obj = obj
        self.strict = strict
    
    def extract(self):
        if type(self.obj) is list:
            return [item.extract() for item in self.obj]
        
        if type(self.obj) is dict:
            return {key: val.extract() for key, val in self.obj.items()}
        
        return self.obj
    
    def check(self, document):
        
        #document = to_doc(document) #TODO: why is this only necessary here and not in other queries?
        if type(self.obj) is dict:
            if not type(document) is dict:
                return False
            
            if self.strict and not set(self.obj) == set(document):
                return False
            
            for key, val in self.obj.items():
                if not key in document:
                    return False
                if not val.check(document[key]):
                    return False
            
            return True
            
        if type(self.obj) is list:
            if not type(document) is list:
                return False
            
            if self.strict and not len(self.obj) == len(document):
                return False
            
            for i, item in enumerate(self.obj):
                if len(document.items) <= i:
                    return False
                if not item.check(document[i]):
                    return False

            return True
        
        return to_doc(self.obj) == document

class Matches(_Equal):
    def __init__(self, obj):
        super().__init__(_make_query(obj), strict = False)

class Exactly(_Equal):
    def __init__(self, obj):
        super().__init__(_make_query(obj), strict = True)

def _make_query(obj):
    if type(obj) is dict:
        return {key:make_query(val) for key,val in obj.items()}
    if type(obj) is list:
        return [make_query(item) for item in obj]
    return obj

def make_query(obj):
    if issubclass(type(obj), Query):
        return obj
    return Exactly(_make_query(obj))
