

import copy
from datajuicer.errors import RangeError
import collections.abc as collections

class Frame(list):
    @staticmethod
    def new():
        return Frame([{}])
    
    def __getitem__(self, item):
        result = list.__getitem__(self, item)
        if type(item) is slice:
            return Frame(result)
        else:
            return result
    
    def __add__(self, rhs):
        return Frame(list.__add__(self, rhs))

def prepare_obj(obj, length):
    if type(obj) is Frame:
        if len(obj) != length:
            raise RangeError
        return obj
    
    f = Frame([copy.copy(obj) for _ in range(length)])
    
    if type(obj) is dict:
        for key, val in obj.items():
            vals = prepare_obj(val, length)
            
            for datapoint,v in zip(f, vals):
                datapoint[key] = v
    elif type(obj) is list:
        for i, val in enumerate(obj):
            vals = prepare_obj(val, length)
            for datapoint,v in zip(f, vals):
                datapoint[i] = v
    return f
    


def configure(frame, configuration, where=None):
    '''
    Returns the frame with all datapoints where the condition holds configured according to the configuration dictionary

            Parameters:
                    frame (Frame of dicts): The frame to be configured
                    configuration (dict): A dictionary with each value possibly being a frame
                    where (iterable of bools): List of booleans that determine which datapoints should be configured

            Returns:
                    configured (Frame of dicts): The configured frame
    '''
    configuration = prepare_obj(configuration, len(frame))

    
    if not where:
        where = [True for _ in frame]
    
    for w in where:
        if not type(w) is bool:
            raise TypeError

    output = Frame([copy.copy(datapoint) for datapoint in frame])
    for cond, dp, conf in zip(where, output, configuration):
        if cond:
            for key in conf:
                dp[key] = conf[key]

    return output

def vary(frame, key, values, where=None):
    '''
    Returns the frame with all datapoints where the condition holds being duplicated such that the key attains each value in values

            Parameters:
                    frame (Frame of dicts): The frame to be varied
                    key (hashable or Frame of hashables): The key that should attain each value
                    values (iterable or Frame of iterables): The values that should be attained
                    where (iterable of bools): List of booleans that determine which datapoints should be varied
            
            Returns:
                    varied (Frame of dicts): The varied frame
    '''
    key = prepare_obj(key, len(frame))
    
    for k in key:
        if not isinstance(k, collections.Hashable):
            return TypeError
    
    values = prepare_obj(values, len(frame))

    if not where:
        where = [True for _ in frame]
    
    for w in where:
        if not type(w) is bool:
            raise TypeError

    for v in values:
        if not type(v) in [list, Frame]:
            raise TypeError
    

    zipped = zip(frame, key)

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


def matches(frame, configuration):
    '''
    Returns a Frame of booleans that indicate which datapoints in the frame match the configuration

            Parameters:
                    frame (Frame of dicts): Frame of dictionaries
                    configuration (dict): A dictionary with each value possibly being a frame
            
            Returns:
                    condition (Frame of bools): Frame of booleans
    '''

    out = Frame([])

    configuration = prepare_obj(configuration, len(frame))


    for conf, datapoint in zip(configuration, frame):
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

def project(frame, keys):
    '''
    Returns a frame with each datapoint having only the specified keys

            Parameters:
                    frame (Frame of dicts): The original frame
                    keys (Frame of iterables of hashables OR Iterable of hashables): The keys that should be kept
            
            Returns:
                    projected (Frame of dicts): The projected frame

    '''
    keys = prepare_obj(keys, len(frame))

    for key in keys:
        for k in key:
            if not isinstance(k, collections.Hashable):
                return TypeError
    
    
    out = Frame([])

    for f,k in zip(frame, keys):
        out.append({key:f[key] for key in k})
    
    return out
            

def select(frame, key):
    '''
    Returns a frame of all values of the specified key

            Parameters:
                    frame (Frame of dicts): The original frame
                    key (hashable OR Frame of hashables): The key that we want to select
            Returns:
                    selection (Frame): Frame with the value of key for each datapoint in frame
    '''
    key = prepare_obj(key, len(frame))

    for k in key:
        if not isinstance(k, collections.Hashable):
            return TypeError
    
    return Frame([data[k] for (data,k) in zip(frame, key)] )