from datajuicer.errors import RangeError
import collections.abc as collections
import datajuicer as dj

class Switch:

    def __init__(self, assignments):
        if not isinstance(assignments, collections.Iterable):
            return TypeError
        self.assignments = dj.Frame(assignments)
    
    def case(self, frame, value):
        if len(frame) != len(self.assignments):
            raise RangeError
        return dj.Frame([datapoint for i, datapoint in enumerate(frame) if self.assignments[i] == value])
    
    def join(self, frames_dict):

        iterators = {}
        for key, frame in frames_dict.items():
            iterators[key] = (f for f in frame)

        out = dj.Frame([])

        for a in self.assignments:
            for key in frames_dict:
                if key==a:
                    out.append(next(iterators[key]))
        
        return out