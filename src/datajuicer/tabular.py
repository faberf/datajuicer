import numpy as np


def _to_dict(_data, dtype=None, **kwargs):

    if dtype is None:
        dtype = type(_data)
    
    if not type(dtype) is str:
        dtype = dtype.__name__
    

    if dtype is "Tabular":
        data = _data.dict
    
    elif dtype is "str":
        data =  {
            "type": "str",
            "data": str(_data)
        }
    else:
        data = {
            "type" : dtype,
            "data" : _data
        }
    
    return {**data, **kwargs}


class Tabular:
    def __init__(self, _data, rows=None, cols=None, **kwargs):
        data = {}
        t = type(_data)
        if t is list:
            for i, item1 in enumerate(_data):
                if type(item1) is list:
                    for j, item2 in enumerate(item1):
                        data[f"{i},{j}"] = _to_dict(item2)
                else:
                    data[f"{i},0"] = _to_dict(item1)
        elif t is np.ndarray:
            if len(_data.shape) == 1:
                for i, item in enumerate(_data):
                    data[f"{i},0"] = _to_dict(item)
            elif len(_data.shape) == 2:
                for i, row in enumerate(_data):
                    for j, item in enumerate(row):
                        data[f"{i},{j}"] = _to_dict(item)
            else:
                raise TypeError
        


        if rows is None:
            rows = "l" * max([int(key.split(",")[0]) for key in data])
        if cols is None:
            cols = "l" * max([int(key.split(",")[1]) for key in data])
        
        
        self.dict = {
            "type" : "Tabular",
            "rows" : rows,
            "cols" : cols,
            "data" : data,
            **kwargs
        }

        trans = {ord(c): None for c in '|'}
        row_num = len(rows.translate(trans))
        col_num = len(cols.translate(trans))
        self.shape = (row_num, col_num)
        self.__dict__.update(kwargs)


