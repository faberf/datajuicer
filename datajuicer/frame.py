

import copy
from datajuicer.errors import RangeError

NoData = None

def _make_frame(obj, length):
    if type(obj) is Frame:
        if len(obj) != length:
            raise RangeError
        return obj
    
    f = Frame([copy.copy(obj) for _ in range(length)])
    
    if type(obj) is dict:
        for key, val in obj.items():
            vals = _make_frame(val, length)
            
            for datapoint,v in zip(f, vals):
                datapoint[key] = v
    elif type(obj) is list:
        for i, val in enumerate(obj):
            vals = _make_frame(val, length)
            for datapoint,v in zip(f, vals):
                datapoint[i] = v
    return f

class Vary:
    def __init__(self, values):
        self.values = values

class BaseFrame:
    def configure(self, configuration):
        pass

    def select(self, key):
        pass

    def where(self, mask):
        pass

    def __getitem__(self, item):
        return self.select(item)

    def __setitem__(self, item, value):
        return self.configure({item:value})
    
    def __eq__(self, other):
        out = []
        other = _make_frame(other, len(self))
        for p1, p2 in zip(self, other):
            if p1 == p2:
                out.append(True)
            else:
                out.append(False)
        return Frame(out)
    
    def map(self, func):
        results = []
        for point in self:
            if type(point) is dict:
                results.append(func(**point))
            else:
                results.append(func(point))
        return Frame(results)
    
    def get(self):
        results = []
        for point in self:
            results.append(point.get())
        return Frame(results)

class Frame(BaseFrame):
    def __init__(self, data=None):
        if data is None:
            data = [{}]
        self.data = data
    
    def __iter__(self):
        return self.data.__iter__()
    
    def __len__(self):
        return len(self.data)

    def select(self, key):
        key = _make_frame(key, len(self))
        ret = []
        for k, d in zip(key, self.data):
            ret.append(d[k])
        return Frame(ret)
    
    def configure(self, configuration):
        cur = Cursor(self, [True for _ in range(len(self))])
        cur.configure(configuration)
        return self
    
    def where(self, mask):
        return Cursor(self, mask)


    def group_by(self, *keys):
        new_data = []
        assignments = []
        for d in self.data:
            new_group = {k:d.get(k, None) for k in keys}
            new = True
            for i, group in enumerate(new_data):
                if group == new_group:
                    assignments.append(i)
                    new = False
                    break
            if new:
                assignments.append(len(new_data))
                new_data.append(new_group)
        all_keys = set().union(*[set(d.keys()) for d in self.data])
        for i,d in enumerate(self.data):
            for k in all_keys:
                if k in keys:
                    continue
                group = new_data[assignments[i]]
                if not k in group:
                    group[k] = []
                v = NoData
                if k in d:
                    v = d[k]
                group[k].append(v)
        
        self.data = new_data
        return self



class Cursor(BaseFrame):
    def __init__(self, frame, mask):
        self.frame = frame
        self.mask = mask
    
    def __len__(self):
        return sum([1 for m in self.mask if m])
    
    def __iter__(self):
        for p, m in zip(self.frame, self.mask):
            if m:
                yield p
    
    def configure(self, configuration):
        configuration = iter(_make_frame(configuration, len(self)))
        new_data = []
        i = 0
        for d, m in zip(self.frame.data, self.mask):
            if m:
                conf = next(configuration)
                variations = [{}]
                for k,v in conf.items():
                    if not type(v) is Vary:
                        v = Vary([v])
                    values = _make_frame(v.values, len(self)).data[i]
                    new_vars = []
                    for var in variations:
                        for val in values:
                            new_var = copy.copy(var)
                            new_var[k] = val
                            new_vars.append(new_var)
                    variations = new_vars
                for variation in variations:
                    new_point = copy.copy(d)
                    for k, v in variation.items():
                        new_point[k] = v
                    new_data.append(new_point)
                i += 1
            else:
                new_data.append(copy.copy(d))
        self.frame.data = new_data
        return self
    
    def select(self, key):
        key = iter(_make_frame(key, len(self)))
        ret = []
        for d, m in zip(self.frame.data, self.mask):
            if m:
                k = next(key)
                ret.append(d[k])
        return Frame(ret)
    
    def where(self, mask):
        mask = iter(mask)
        new = []
        for m in self.mask:
            if m:
                if next(mask):
                    new.append(True)
                    continue
            new.append(False)

        return Cursor(self.data, new)