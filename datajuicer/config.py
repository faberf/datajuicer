import copy
import inspect


class Config:
    class Undefined:
        pass
    
    class Section:
        def __init__(self, cond_func, **raw_node_dict):
            self.cond_func = cond_func
            self.raw_node_dict = raw_node_dict
            self.parents = inspect.getfullargspec(self.cond_func).args

    class Param:
        def __init__(self, func, parents =None):
            def make_callable(func):
                return lambda : func
            if not callable(func):
                func = make_callable(func)
            self.func = func
            if parents is None:
                parents = inspect.getfullargspec(self.func).args
            self.parents = parents
    
    class Collection:

        def __init__(self, *keys, **renamed_keys):
            

            self.renamed_keys = renamed_keys
            self.keys = keys


            self.parents = [key for key in keys]

            for original in renamed_keys.values():
                self.parents.append(original)
            
            self.func = lambda **kwargs: {**{key:kwargs[key] for key in self.keys if key in kwargs},**{renaming:kwargs[original] for renaming, original in self.renamed_keys.items() if original in kwargs}}
            
    def add(self, *sections, **raw_node_dict):
        def with_section(node, sec):
            def make(**kwargs):
                if sec.cond_func(**{key: kwargs[key] for key in sec.parents}):
                    return node.func(**{key: kwargs[key] for key in node.parents})
                return Config.Undefined
            return make

        for sec in sections:
            for key, node in Config(**sec.raw_node_dict).node_dict.items():
                if not key in self.duplicate_node_dict:
                    self.duplicate_node_dict[key] = []
                self.duplicate_node_dict[key].append(Config.Param(
                    with_section(node, sec),
                    node.parents + sec.parents
                    )
                )

        def wrap(val):
            return lambda: val

        for key, val in raw_node_dict.items():
            if not type(val) in [Config.Param, Config.Collection] :
                val = Config.Param(wrap(val))
            if not key in self.duplicate_node_dict:
                self.duplicate_node_dict[key] = []
            self.duplicate_node_dict[key].append(val)
        
        self.node_dict = {
            **{f"{key}__{i}" : val for key, duplicates in self.duplicate_node_dict.items() for i, val in enumerate(duplicates)},
            **{key: Config.Param(lambda kwargs: kwargs.values()[-1], [f"{key}__{i}" for i, _ in enumerate(duplicates)]) for key, duplicates in self.duplicate_node_dict.items()}
        
        }
        

    def __init__(self, *sections,**raw_node_dict):
        self.duplicate_node_dict = {}
        self.node_dict = {}
        self.add(*sections, **raw_node_dict)
        
            
    
    def populate(self, **value_dict):
        output_dict = {key:val for key, val in value_dict.items() if key in self.node_dict}
        undefined = []
        
        progress = 0
        while not all([key in output_dict for key in self.node_dict if not key in undefined]):
            
            for key, node in self.node_dict.items():
                if key in output_dict:
                    continue
                if all([parent in output_dict for parent in node.parents if not parent in undefined]):
                    result = node.func(**{key:output_dict[key] for key in node.parents if not key in undefined})
                    if not result is Config.Undefined:
                        output_dict[key] = result
                    else:
                        undefined.append(key)
            
            new_progress = len(output_dict) + len(undefined)
            if progress == new_progress:
                raise Exception("Not Enough Information to Populate Configuration")
            progress = new_progress
            
        return output_dict
    
    def __iter__(self):
        for key, val in self.node_dict.items():
            yield (key, val)