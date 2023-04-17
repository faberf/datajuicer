import inspect
import importlib
import pathlib


def make_hidden_name(func_name):
    return f"__{func_name}"


class Function:
    """A function that can be serialized and deserialized.
    """
    def __init__(self, func):
        self.name = func.__name__
        self.file = func.__globals__['__file__']
    
    def get_func(self):
        spec = importlib.util.spec_from_file_location(pathlib.Path(self.file).stem, self.file)
        foo = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(foo) # We execute the module every time we call the function. This is not efficient, but I don't know how to do it otherwise.
        return getattr(foo, self.name)
    
    def __call__(self, *args, **kwargs):
        return self.get_func()(*args, **kwargs)
    
    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, Function):
            return False
        return self.name == __value.name and self.file == __value.file

    
    # def apply_defaults(self, *args, **kwargs):
    #     sig = inspect.signature(self.get_func())

    #     boundargs = sig.bind(*args,**kwargs)
    #     boundargs.apply_defaults()

    #     return dict(boundargs.arguments)