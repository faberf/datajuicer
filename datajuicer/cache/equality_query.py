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
        
        if type(self.obj) is dict:
            if not type(document) is dict:
                return False
            
            if self.strict and not set(self.obj) == set(document):
                return False
            
            for key, val in self.obj.items():
                val = NoDocument
                if key in document:
                    val = document[key]
                if not val.check(val):
                    return False
            
            return True
            
        if type(self.obj) is list:
            if not type(document) is list:
                return False
            
            if self.strict and not len(self.obj) == len(document):
                return False
            
            for i, item in enumerate(self.obj):
                val = NoDocument
                if len(document) > i:
                    val = document[i]
                if not item.check(val):
                    return False

            return True
        
        return to_doc(self.obj) == document

class Matches(_Equal):
    """A query that matches any document that agrees with the query on all fields they share in common. If the object is a list, the query will match any document with a prefix that matches the list.
    """
    def __init__(self, obj):
        super().__init__(_make_query(obj), strict = False)

class Exactly(_Equal):
    """A query that matches any document that has the same fields as the given object. The values of the fields must match the values of the object. The object can be a dictionary or a list. If the object is a dictionary, the query will match any document that has the same keys as the object and the values of the keys match the values of the object. If the object is a list, the query will match any document that has the same length as the object and the values of the items match the values of the object.
    """
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
