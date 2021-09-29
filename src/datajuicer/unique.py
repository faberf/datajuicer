import copy
import datajuicer as dj

class Unique:

    def __init__(self, frame):
        data = dj.Frame([])
        projection = dj.Frame([])
        is_canonical = dj.Frame([])
        for f in frame:
            unique = True
            for i, uf in enumerate(data):
                if f == uf:
                    unique = False
                    projection.append(i)
                    is_canonical.append(False)
                    break
            if unique:
                projection.append(len(data))
                data.append(copy.copy(f))
                is_canonical.append(True)
                
        self.projection = projection
        self.data = data
        self.where_canonical = dj.Where(is_canonical)
    
    def expand(self, frame):
        out = dj.Frame([])
        for p in self.projection:
            out.append(copy.copy(frame[p]))
        return out