
import numpy as np

class Requirement:
    def __init__(self, obj):
        self.obj = obj
    
    def __eq__(self, __o: object) -> bool:
        return self.obj.__eq__(__o)

class Any(Requirement):
    def __eq__(self, __o: object) -> bool:
        return True
    
    def __hash__(self) -> int:
        from datajuicer.hash import get_hash
        return hash((Any, get_hash(self.obj)))

class Close(Requirement):
    def __eq__(self, __o: object) -> bool:
        return np.isclose(self.obj, __o)
    
    def __hash__(self) -> int:
        from datajuicer.hash import get_hash
        return hash((Close, get_hash(self.obj)))

class Matches(Requirement):
    def __eq__(self, __o):
        for key, val in self.obj.items():
            if not key in __o:
                return False
            if not __o[key] == val:
                return False
        return True
    

    def __hash__(self) -> int:
        from datajuicer.hash import get_hash
        return hash((Matches, get_hash(self.obj)))

def recurse(obj,base_case):
    if type(obj) is dict:
        return {key:recurse(val, base_case) for key, val in obj.items()}

    if type(obj) is list:
        return [recurse(val, base_case) for i, val in enumerate(obj)]
    
    return base_case(obj)

    


def extract(obj):
    def _extract(obj):
        if issubclass(type(obj), Requirement):
            ret = obj.obj
            if type(ret) is dict or type(ret) == list:
                ret = extract(ret)
            return ret
        return obj
    return recurse(obj, _extract)