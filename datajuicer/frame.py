

import copy
from datajuicer.errors import RangeError

NoData = None

def _make_frame(obj, length):
    if issubclass(type(obj), BaseFrame):
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

def _frame_length(obj):
    if issubclass(type(obj), BaseFrame):
        return len(obj)
    
    if type(obj) is dict:
        for val in obj.values():
            l = _frame_length(val)
            if not l is None:
                return l
    if type(obj) is list:
        for val in obj:
            l = _frame_length(val)
            if not l is None:
                return l

def _is_normal(obj):
    if issubclass(type(obj), BaseFrame) or issubclass(type(obj), Vary):
        return False

    if type(obj) is dict:
        for val in obj.values():
            if not _is_normal(val):
                return False
    if type(obj) is list:
        for val in obj:
            if not _is_normal(val):
                return False
    return True

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
    def join(self):
        for point in self:
            point.join()
        return self
    
    def load(self, filename, mode, load_func, *args, **kwargs):
        results = []
        for point in self:
            results.append(point.load(filename, mode, load_func, *args, **kwargs))
        return Frame(results)


class Frame(BaseFrame):
    @staticmethod
    def make(__frame_len = None, **kwargs):
        if __frame_len is None:
            __frame_len = _frame_length(kwargs)
        if __frame_len is None:
            f = Frame()
        else:
            f = Frame([{} for _ in range(__frame_len)])
        f.configure(kwargs)
        l = []
        for d in f:
            if _is_normal(d):
                l.append(d)
                continue
            for key, val in d.items():
                if not _is_normal(val):
                    d[key] = Vary(list(Frame.make(**val)))
            d_frame = Frame.make(**d)
            l += list(d_frame)
        return Frame(l)

    def __init__(self, data=None):
        if data is None:
            data = [{}]
        self.data = data
    
    def __iter__(self):
        return self.data.__iter__()
    
    def __len__(self):
        return len(self.data)

    def select(self, key):
        cur = Cursor(self, [True for _ in range(len(self))])
        return cur.select(key)
    
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
    def __init__(self, frame, mask, path = ()):
        self.frame = frame
        self.mask = mask
        self.path_frame = _make_frame(path, len(self)) #len(self)?
    
    def __len__(self):
        return sum([1 for m in self.mask if m])
    
    def __iter__(self):
        path_iter = iter(self.path_frame)
        for p, m in zip(self.frame, self.mask):
            if m:
                path = next(path_iter)
                for key in path:
                    p = p[key]
                yield p
    
    def configure(self, configuration):
        configuration = iter(_make_frame(configuration, len(self)))
        new_data = []
        i = 0
        path_iter = iter(self.path_frame)
        for d, m in zip(self.frame.data, self.mask):
            
            if m:
                path = next(path_iter)
                conf = next(configuration)
                # convert_back_to_list = type(conf) is list
                # if convert_back_to_list:
                #     conf = dict(enumerate(conf))
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
                    orig_new_point = copy.deepcopy(d)
                    new_point = orig_new_point
                    for key in path:
                        new_point = new_point[key]
                    for k, v in variation.items():
                        new_point[k] = v
                    new_data.append(orig_new_point)
                i += 1
            else:
                new_data.append(copy.copy(d))
        self.frame.data = new_data
        return self
    
    def select(self, key):
        new_path_frame = []
        for path, k in zip(self.path_frame, _make_frame(key, len(self))):
            new_path_frame.append(path + (k,))
        return Cursor(self.frame, self.mask, Frame(new_path_frame))
    
    def where(self, mask):
        mask = iter(mask)
        new = []
        for m in self.mask:
            if m:
                if next(mask):
                    new.append(True)
                    continue
            new.append(False)

        return Cursor(self.frame, new)
