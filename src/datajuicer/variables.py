from os import name, replace
from typing import OrderedDict
import datajuicer as dj
import itertools
import numpy as np

class BaseVariable:
    
    def iterate(self, data):
        pass

    def num_values(self, data):
        return len(list(self.iterate(data)))
    
    def __eq__(self, o: object) -> bool:
        return type(o) is type(self)

class Variable(BaseVariable):
    def __init__(self, *keys):
        if not type(keys) in [tuple, list]:
            self.keys = (keys,)
        else:
            self.keys = tuple(keys)
    

    def iterate(self, data):
        values = dj.Unique({key:data.select(key) for key in self.keys}).data
        for val in values:
            if len(val) == 1:
                v =  list(val.values())[0]
            else:
                v = tuple(val.values())
            yield v, dj.Where(data.matches(val)).true(data)
    
    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and self.keys == o.keys

class SelectionVariable(BaseVariable):

    def __init__(self, key):
        self.key = key
        r = lambda f: (key, dj.select(f, key))
        self.var = ReductionsVariable((r,))
    
    def iterate(self, data):
        yield self.key + next(self.var.iterate(data))
    
    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and self.key == o.key

class ChainVariable(BaseVariable):
    def __init__(self, *vars):
        self.vars = vars

    def iterate(self, data):
        return itertools.chain(*(v.iterate(data) for v in self.vars))
    
    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and self.vars == o.vars

class ReductionsVariable(BaseVariable):
    class Default:
        pass
    def __init__(self, *reductions):
        def get_name_and_func(r):
            if type(r) is type:
                if issubclass(r, dj.Task):
                    return r.name, r.reduce
            if callable(r):
                return r.__name__, r
            raise TypeError

        self.reductions = {}
        for reduc in reductions:
            t = type(reduc)
            if t is dict:
                for key, val in reduc.items():
                    default_key, func = get_name_and_func(val)
                    if type(key) is ReductionsVariable.Default:
                        key = default_key
                    self.reductions[key] = func
            else:
                name, func = get_name_and_func(reduc)
                self.reductions[name] = func
    
    def iterate(self, data):
        for n, r in self.reductions.items():
            yield n, r(data)
    
    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and self.reductions.keys() == o.reductions.keys()

class JointVariable(BaseVariable):
    
    def __init__(self, *vars):
        self.vars = vars
    
    def iterate(self, data):
        if len(self.vars) == 1:
            for val, d in self.vars[0].iterate(data):
                yield (val,), d
            return
        
        for val, d in self.vars[0].iterate(data):
            pvar = JointVariable(*self.vars[1:])
            for vals, d1 in pvar.iterate(d):
                yield (val,) + vals, d1
    
    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and all([var1==var2 for (var1,var2) in zip(self.vars, o.vars)])
    

class ProductVariable(BaseVariable):
    def __init__(self, *vars):
        self.vars = vars
    
    def iterate(self, data):
        if len(self.vars) == 1:
            for val, d in self.vars[0].iterate(data):
                yield val, (d,)
            return
        
        for val, d in self.vars[0].iterate(data):
            pvar = JointVariable(*self.vars[1:])
            for vals, d1 in pvar.iterate(d):
                yield val, (d1,) + d
    
    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and all([var1==var2 for (var1,var2) in zip(self.vars, o.vars)])

class NoData:
    def __iter__(self):
        for i in range(2):
            yield NoData

class DummyVariable(BaseVariable):
    def iterate(self, data):
        yield NoData, data
    

class Permutation(BaseVariable):
    def __init__(self, var, vals):
        self.var = var
        self.vals = tuple(vals)
    
    def iterate(self, data):
        dict = {val:d for val, d in self.var.iterate()}
        for val in self.vals:
            yield val, dict[val]
    
    def __eq__(self, o: object) -> bool:
        return super().__eq__(o) and self.var == o.var and self.vals == o.vals


class IndexVariable(BaseVariable):
    def __init__(self):
        pass

    def iterate(self, data):
        return enumerate(data)
    
    def __eq__(self, o: object) -> bool:
        return super().__eq__(o)
   
class BaseFormatter:

    def __init__(self):
        pass

    def format_axis(self, variable):
        return type(variable).__name__

    def format_value(self, value, variable):
        return str(value)

    def format_datapoint(self, datapoint):
        return str(datapoint)

def make_list_from_shape(shape):
    if len(shape) == 1:
        return [None for _ in range(shape[0])]
    
    return [make_list_from_shape(shape[1:]) for _ in range(shape[0])]

class Table:
    def __init__(self, dims, data, vars, formatter=None, metadata = None):
        len_vars = len(vars)
        if len_vars > dims:
            var = JointVariable(JointVariable(vars[0:len_vars-dims]), *vars[len_vars-dims:])
        else:
            var = JointVariable(*[DummyVariable() for _ in range(dims-len_vars)], *vars)
        if formatter is None:
            formatter = BaseFormatter()
        summarized = (*var.iterate(data),)
        vals_tuples, datapoints = zip(*summarized)
        vals_sets = [list(dj.Unique(dj.Frame(vs)).data) for vs in zip(*vals_tuples)]
        self.shape = (*(len(vals_set) for vals_set in vals_sets),)
        self._grid = np.full(self.shape, None, dtype=object)
        if metadata == None:
            metadata = {}
            
        axes_vals = [[] for _ in self.shape]
        for i,l in enumerate(self.shape):
            for j in range(l):
                axes_vals[i].append(formatter.format_value(vals_sets[i][j], var.vars[i], **metadata))
        self.axes = tuple(tuple(av) for av in axes_vals)
        self.axis_names = tuple(formatter.format_axis(var, **metadata) for var in var.vars)
        for vs, dp in summarized:
            idxs = tuple(ax.index(v) for (ax,v) in zip(vals_sets, vs))
            self[idxs] = formatter.format_datapoint(dp, **metadata)
        
    def __getitem__(self, key):
        return self._grid.__getitem__(key)
    
    def __setitem__(self, key, val):
        return self._grid.__setitem__(key, val)
    
    def __iter__(self):
        return self._grid.__iter__()
    
    @staticmethod
    def make(dims, **metadata):
        def decorator(func):
            def output(data, vars, formatter=BaseFormatter(), **kwargs):
                return func(Table(dims, data, vars, formatter, metadata), **kwargs)
            
            return output
        
        return decorator
