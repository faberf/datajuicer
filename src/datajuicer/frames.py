

import copy
from datajuicer.errors import RangeError
import collections

class Frame(list):
    @staticmethod
    def new():
        return Frame([{}])

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
    configuration = {key:([value for _ in frame] if not type(value) is Frame else value) for (key, value) in configuration.items()}

    for val in configuration.values():
        if len(val) != len(frame):
            raise RangeError
    
    if not where:
        where = [True for _ in frame]
    
    for w in where:
        if not type(w) is bool:
            raise TypeError

    output = [copy.copy(datapoint) for datapoint in frame]
    for key, val_list in configuration.items():
        for i, (val, datapoint) in enumerate(zip(val_list, output)):
            if where[i]:
                datapoint[key] = val

    return Frame(output)

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
    if not type(key) is Frame:
        key = [key for _ in frame]
    
    for k in key:
        if not isinstance(k, collections.Hashable):
            return TypeError
    
    if not type(values) is Frame:
        values = [values for _ in frame]

    if not where:
        where = [True for _ in frame]
    
    for w in where:
        if not type(w) is bool:
            raise TypeError

    for v in values:
        if not type(v) in [list, Frame]:
            raise TypeError
    
    if len(key) != len(frame) or len(values) != len(frame) or len(where) != len(frame):
        raise RangeError

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

    configuration = {key:([value for _ in frame] if not type(value) is Frame else value) for (key, value) in configuration.items()}

    for val in configuration.values():
        if len(val) != len(frame):
            raise RangeError

    for i, datapoint in enumerate(frame):
        for key in configuration:
            matches = True
            if not key in datapoint:
                matches = False
                break
            if datapoint[key] != configuration[key][i]:
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
    if not type(keys) is Frame:
        keys = [keys for _ in frame]

    for key in keys:
        for k in key:
            if not isinstance(k, collections.Hashable):
                return TypeError
    
    if not len(keys) == len(frame):
        raise RangeError
    
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
    if not type(key) is Frame:
        key = [key for _ in frame]

    for k in key:
        if not isinstance(k, collections.Hashable):
            return TypeError

    if len(key) != len(frame):
        raise RangeError
    
    return Frame([data[k] for (data,k) in zip(frame, key)] )