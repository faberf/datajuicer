from datajuicer.errors import RangeError
import datajuicer as dj

class Where:

    def __init__(self, condition):
        if not all([type(c) is bool for c in condition]):
            raise TypeError

        self.condition = condition
    
    def true(self, frame):
        if len(frame) != len(self.condition):
            raise RangeError
        return dj.Frame([datapoint for i, datapoint in enumerate(frame) if self.condition[i]])
    
    def false(self, frame):
        if len(frame) != len(self.condition):
            raise RangeError
        return dj.Frame([datapoint for i, datapoint in enumerate(frame) if not self.condition[i]])
    
    def join(self, true, false):

        tit = (f for f in true)
        fit = (f for f in false)

        out = dj.Frame([])

        for b in self.condition:
            if b:
                out.append(next(tit))
            else:
                out.append(next(fit))
        
        return out