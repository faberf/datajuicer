import inspect
import importlib
import pathlib

def make_hidden_name(func_name):
    return f"__{func_name}"


class Function:
    def __init__(self, func):
        self.name = func.__name__
        self.file = func.__globals__['__file__']
    
    def get_func(self):
        spec = importlib.util.spec_from_file_location(pathlib.Path(self.file).stem, self.file)
        foo = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(foo) # TODO: Do we want to be executing it everytime
        return getattr(foo, self.name)
    
    def __call__(self, *args, **kwargs):
        return self.get_func()(*args, **kwargs)

    
    # def apply_defaults(self, *args, **kwargs):
    #     sig = inspect.signature(self.get_func())

    #     boundargs = sig.bind(*args,**kwargs)
    #     boundargs.apply_defaults()

    #     return dict(boundargs.arguments)