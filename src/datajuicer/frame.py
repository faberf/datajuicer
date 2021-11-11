

import copy
from datajuicer.errors import NoFramesError, RangeError
import collections.abc as collections
import string
import random
from datajuicer.utils import rand_id

class Frame:
    def __init__(self, obj = None):
        if obj is None:
            self.data = {rand_id(): {}}
        else:
            self.data = {rand_id():point for point in obj}
    
    def __iter__(self):
        for val in self.data.values():
            yield val
    
    def __len__(self):
        return len(self.data)
    
    def _cursor(self):
        return FrameCursor(self, self.data.keys())

    def __getitem__(self, obj):
        return self._cursor().__getitem__(obj)
    
    def __setitem__(self, key, val):
        return self._cursor().__setitem__(key, val)
    
    def __add__(self, obj):
        return self._cursor() + obj
    
    def __and__(self, other):
        return self._cursor() and other
    
    def __or__(self, other):
        return self._cursor() or other
    
    def __inv__(self, other):
        return ~self._cursor()
    
    def __eq__(self, other):
        return self._cursor() == other
    
    def configure(self, configuration):
        return self._cursor().configure(configuration)
    
    def vary(self, key, values):
        return self._cursor().vary(key, values)
    
    def where(self, frame):
        return self._cursor().where(frame)
    
    def select(self, key):
        return self._cursor.select(key)
    
    @staticmethod
    def make(obj, length=None):
        if type(obj) is FrameCursor:
            return obj.frame
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


class FrameCursor:
    def __init__(self, frame, points):
        self.frame = frame
        self.points = list(points)
    
    def __getitem__(self, obj):
        if type(obj) is Frame:
            return self.where(obj)
        else:
            return self.select(obj)
    
    def __setitem__(self, key, val):
        return self.configure({key:val})
    
    def __add__(self, obj):
        return Frame(list(Frame.make(self)) + list(Frame.make(obj)))
    
    def __and__(self, other):
        return Frame([p1 and p2 for p1,p2 in zip(Frame.make(self),Frame.make(other, len(self.points)))])
    
    def __or__(self, other):
        return Frame([p1 or p2 for p1,p2 in zip(Frame.make(self),Frame.make(other, len(self.points)))])
    
    def __inv__(self, other):
        return Frame([not point for point in Frame.make(self)])
    
    def __eq__(self, other):
        return Frame([p1 == p2 for p1,p2 in zip(Frame.make(self), Frame.make(other, len(self.points)))])
    
    def where(self, frame):
        new_points = []
        for point, keep in zip(self.points, frame):
            if keep:
                new_points.append(point)
        self.points = new_points
        return self
    
    def configure(self, configuration):
        configuration = Frame.make(configuration, len(self.points))
        for point, conf in zip(self.points, configuration):
            for key in conf:
                self.frame.data[point][key] = conf[key]
        return self
    
    def vary(self, key, values):
        key = Frame.make(key, len(self.points))
        
        for k in key:
            if not isinstance(k, collections.Hashable):
                return TypeError
        
        values = Frame.make(values, len(self.points))

        for k, vals, point in zip(key, values, self.points):
            for val in vals:
                new_point = copy.copy(self.frame.data[point])
                new_point[k] = val
                self.frame.data[rand_id()] = new_point
            del self.frame.data[point]

        return self
    
    def select(self, key):
        key = Frame.make(key, len(self.points))

        for k in key:
            if not isinstance(k, collections.Hashable):
                return TypeError
        
        return Frame([self.frame.data[point][k] for (point,k) in zip(self.points, key)] )

    def filter(self):
        for point in self.frame.data:
            if not point in self.points:
                del self.frame.data[point]


