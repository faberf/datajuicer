import datajuicer.dependency as dependency
from datajuicer.context import Context
from datajuicer.function import Function
import datajuicer.requirements as requirements
import threading

def config(raw = False):
    return lambda func: Configuration(func, raw)


local = threading.local()
local.counter = 0

class Configuration(Function):
    def __init__(self, func, raw):
        super().__init__(func)
        self.raw = raw
    
    def evaluate(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)
    
    def __call__(self, *args, **kwargs):
        local.counter += 1
        ret = self.evaluate(*args, **kwargs)
        local.counter -= 1
        if local.counter == 0:
            if not Context.get_active().incognito:
                Context.get_active().run.add_dependency(dependency.ConfigDependency(self, args, kwargs, ret))
        if not self.raw:
            ret = requirements.extract(ret)
        return ret
    
