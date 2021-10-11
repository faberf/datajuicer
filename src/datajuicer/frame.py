

import copy
from datajuicer.errors import NoFramesError, RangeError
import collections.abc as collections

class Frame(list):

    def __init__(self, *args, **kwargs):
        if len(args) + len(kwargs) == 0:
            return super().__init__([{}])
        return super().__init__(*args, **kwargs)

   
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

    
    
    def configure(self, configuration, where=None):
        '''
        Returns the frame with all datapoints where the condition holds configured according to the configuration dictionary

                Parameters:
                        self (Frame of dicts): The frame to be configured
                        configuration (dict): A dictionary with each value possibly being a frame
                        where (iterable of bools): List of booleans that determine which datapoints should be configured

                Returns:
                        configured (Frame of dicts): The configured frame
        '''
        configuration = Frame.make(configuration, len(self))

        
        if not where:
            where = [True for _ in self]
        
        for w in where:
            if not type(w) is bool:
                raise TypeError

        output = Frame([copy.copy(datapoint) for datapoint in self])
        for cond, dp, conf in zip(where, output, configuration):
            if cond:
                for key in conf:
                    dp[key] = conf[key]

        return output

    def vary(self, key, values, where=None):
        '''
        Returns the frame with all datapoints where the condition holds being duplicated such that the key attains each value in values

                Parameters:
                        self (Frame of dicts): The frame to be varied
                        key (hashable or Frame of hashables): The key that should attain each value
                        values (iterable or Frame of iterables): The values that should be attained
                        where (iterable of bools): List of booleans that determine which datapoints should be varied
                
                Returns:
                        varied (Frame of dicts): The varied frame
        '''
        key = Frame.make(key, len(self))
        
        for k in key:
            if not isinstance(k, collections.Hashable):
                return TypeError
        
        values = Frame.make(values, len(self))

        if not where:
            where = [True for _ in self]
        
        for w in where:
            if not type(w) is bool:
                raise TypeError

        for v in values:
            if not type(v) in [list, Frame]:
                raise TypeError
        

        zipped = zip(self, key)

        output = []
        for i, (datapoint, key) in enumerate(zipped):
            if where[i]:
                for j in range(len(values[i])):
                    copied = copy.copy(datapoint)
                    copied[key] = values[i][j]
                    output.append(copied)
            else:
                output.append(copy.copy(datapoint))
            
        return Frame(output)


    def matches(self, configuration):
        '''
        Returns a Frame of booleans that indicate which datapoints in the frame match the configuration

                Parameters:
                        self (Frame of dicts): Frame of dictionaries
                        configuration (dict): A dictionary with each value possibly being a frame
                
                Returns:
                        condition (Frame of bools): Frame of booleans
        '''

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

    def project(self, keys):
        '''
        Returns a frame with each datapoint having only the specified keys

                Parameters:
                        self (Frame of dicts): The original frame
                        keys (Frame of iterables of hashables OR Iterable of hashables): The keys that should be kept
                
                Returns:
                        projected (Frame of dicts): The projected frame

        '''
        keys = Frame.make(keys, len(self))

        for key in keys:
            for k in key:
                if not isinstance(k, collections.Hashable):
                    return TypeError
        
        
        out = Frame([])

        for f,k in zip(self, keys):
            out.append({key:f[key] for key in k})
        
        return out
                

    def select(self, key):
        '''
        Returns a frame of all values of the specified key

                Parameters:
                        self (Frame of dicts): The original frame
                        key (hashable OR Frame of hashables): The key that we want to select
                Returns:
                        selection (Frame): Frame with the value of key for each datapoint in frame
        '''
        key = Frame.make(key, len(self))

        for k in key:
            if not isinstance(k, collections.Hashable):
                return TypeError
        
        return Frame([data[k] for (data,k) in zip(self, key)] )
    
    def __getitem__(self, item):
        result = list.__getitem__(self, item)
        if type(item) is slice:
            return Frame(result)
        else:
            return result
    
    def __add__(self, rhs):
        return Frame(list.__add__(self, rhs))

