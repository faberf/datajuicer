

import copy
# from datajuicer.errors import NoFramesError, RangeError
import collections.abc as collections
import string
import random

ID_LEN = 10

def rand_id():
    letters = string.ascii_letters + string.digits
    return ''.join(random.choice(letters) for i in range(ID_LEN))

class NoFramesError:
    pass

class RangeError:
    pass

class Frame:
    def __init__(self, obj = None):
        self.order = []
        self.data = {}
        self.cursor = []

        if obj is None:
            self.append({})
        elif type(obj) is Frame:
            self.order = copy.copy(obj.order)
            self.data = {point_id:obj.data[point_id] for point_id in self.order}
        else:
            for point in obj:
                self.append(point)
        
        self.cursor = copy.copy(self.data)
    
    def _subframe(self, point_ids):
        f = Frame([])
        f.data = self.data
        f.order = self.order
        f.cursor = point_ids
        return f
    
    def append(self, obj):
        point_id = rand_id()
        self.order.append(point_id)
        self.data[point_id] = obj

    def __len__(self):
        return len(self.cursor)
    
    def __iter__(self):
        for point_id in self.cursor:
            yield self.data[point_id]
    
    @staticmethod
    def make(obj=None, length=None):
        if obj is None:
            return Frame.new()
        if length is None:
            length = Frame.length(obj)
        if type(obj) is Frame:
            if len(obj) != length:
                raise RangeError
            return obj
        
        f = Frame([copy.copy(obj) for _ in range(length)])
        
        if type(obj) is dict:
            for key, val in obj.items():
                vals = Frame.make(val, length)
                
                for datapoint,v in zip(f, vals):
                    datapoint[key] = v
        elif type(obj) is list:
            for i, val in enumerate(obj):
                vals = Frame.make(val, length)
                for datapoint,v in zip(f, vals):
                    datapoint[i] = v
        return f
    
    @staticmethod
    def length(obj):
        def _frame_length(obj):
            if type(obj) is Frame:
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
        length = _frame_length(obj)
        if length is None:
            raise NoFramesError
        return length

    
    def __mod__(self, configuration):
        out = Frame([])

        configuration = Frame.make(configuration, len(self))

        for conf, datapoint in zip(configuration, self):
            for key in conf:
                matches = True
                if not key in datapoint:
                    matches = False
                    break
                if datapoint[key] != conf[key]:
                    matches = False
                    break
            out.append(matches)
        
        return out
    
    def __add__(self, other):
        out = Frame([])
        for point in self:
            out.append(point)
        for point in other:
            out.append(point)
        return out
    
    def __and__(self, other):
        return Frame([p1 and p2 for p1,p2 in zip(self,other)])
    
    def __or__(self, other):
        return Frame([p1 or p2 for p1,p2 in zip(self,other)])
    
    def __inv__(self, other):
        return Frame([not point for point in self])
    
    def __eq__(self, other):
        other = Frame.make(other, len(self))
        return Frame([p1 == p2 for p1,p2 in zip(self, other)])

    def where(self, frame):
        point_ids = []
        for point_id, val in zip(self.cursor, frame):
            if val:
                point_ids.append(point_id)
        return self._subframe(point_ids)
    
    def select(self, key):
        key = Frame.make(key, len(self))

        for k in key:
            if not isinstance(k, collections.Hashable):
                return TypeError
        
        return Frame([data[k] for (data,k) in zip(self, key)] )

    def __getitem__(self, obj):
        if type(obj) is Frame:
            return self.where(obj)
        else:
            return self.select(obj)
    
    def __setitem__(self, key, val):
        conf = Frame.make({key: val}, len(self))
        return self.configure(conf)
    
    def configure(self, configuration):
        configuration = Frame.make(configuration, len(self))
        for point_id, conf in zip(self.cursor, configuration):
            for key in conf:
                self.data[point_id][key] = conf[key]
        return self
    
    def vary(self, key, values):
        key_frame = Frame.make(key, len(self))
        
        for k in key_frame:
            if not isinstance(k, collections.Hashable):
                return TypeError
        
        values_frame = Frame.make(values, len(self))

        all_point_ids = []

        for point_id, key, values in zip(self.cursor, key_frame, values_frame):
            point_ids = [rand_id() for _ in values]
            all_point_ids += point_ids
            idx = self.order.index(point_id)
            self.order[idx:idx+1] = point_ids
            for val, pid in zip(values, point_ids):
                self.data[pid] = copy.copy(self.data[point_id])
                self.data[pid][key] = val
            del self.data[point_id]
        self.cursor = all_point_ids
        return self

f = Frame()
f.configure({"cheese":2})
f["cheese"] = 4
f.vary("bacon", [1,2,3,4])
ff = f.where(f["bacon"]==1)
fff = f.where(f["bacon"] == 1).vary("cheese", [5,6])
print("hello")



    

    

