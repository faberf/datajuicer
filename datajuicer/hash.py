import dill as pickle
import datajuicer.requirements as requirements
import datajuicer.run as run

def serializeable(obj):
    if issubclass(type(obj),requirements.Requirement):
        return type(obj)(serializeable(obj.obj))
    if type(obj) is dict:
        out = {}
        for key, val in obj.items():
            out[serializeable(key)] = serializeable(val)
        return out
    
    if type(obj) is list:
        out = []
        for item in obj:
            out.append(serializeable(item))
        return out
    
    # if type(obj) is tuple:
    #     out = []
    #     for item in obj:
    #         out.append(_serialize(item))
    #     return tuple(out)
    
    if type(obj) in [int, float, bool]:
        return obj
    if type(obj) is str:
        return "str_" + obj
    if issubclass(type(obj), run.Run):
        return "run_" + obj.run_id
    if callable(obj) and hasattr(obj, "__module__") and hasattr(obj, "__name__"):
        return f"func_{obj.__module__}_{obj.__name__}"
    if obj is None:
        return "none"
    h = hash(pickle.dumps(obj))
    return f"hash_{h}"


def get_hash(request):
    ser = serializeable(request)

    def _hash(obj):
        if type(obj) is dict:
            return tuple(("dict",*sorted((_hash(key), _hash(val)) for key, val in obj.items())))
        
        if type(obj) is list:
            return tuple(("list",*(_hash(it) for it in obj)))
        
        if type(obj) is tuple:
            return tuple(("tuple",*(_hash(it) for it in obj)))
        
        return obj

    return hash(_hash(ser))