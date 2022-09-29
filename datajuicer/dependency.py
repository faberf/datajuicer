from datajuicer.hash import serializeable
import datajuicer.run as run
import dill as pickle
import datajuicer.requirements as requirements


class CheckResult:
    class Bad:
        pass
    class Pending:
        pass
    class Good:
        pass
    class Crashed:
        pass

class LocalDependency:
    
    def check(self, run, request):
        return CheckResult.Good

class GlobalDependency:

    def check(self, run, _):
        return CheckResult.Good

class ImpossibleDependency(LocalDependency):
    def check(self, run, _):
        return CheckResult.Bad

class ConfigDependency(GlobalDependency):

    def __init__(self, config, args, kwargs, output):
        self.config = config
        self.args = args
        self.kwargs = kwargs
        self.output = serializeable(output)

    def check(self, run, _):
        if serializeable(self.config.evaluate(*self.args, **self.kwargs)) == self.output:
            return CheckResult.Good
        return CheckResult.Bad

import copy
class ParamDependency(LocalDependency):
    
    def __init__(self, kwargs):
        self.deps = serializeable(requirements.extract(kwargs))
    
    def check(self, run, request):

        if check_equality(serializeable(request.parameters),self.deps):
            return CheckResult.Good
        return CheckResult.Bad

def check_equality(a, b):

    if type(a) == dict:
        for key in list(a) + list(b):
            if not type(b) is dict:
                return False
            if not key in a:
                return False
            if type(a[key]) == requirements.Any:
                continue
            if not key in b:
                return False

            if not check_equality(a[key], b[key]):
                return False
        return True
    
    return a == b
class DoneDependency(LocalDependency):

    
    def check(self, run, request):

        state = run.get_state()

        if state == "done":
            return CheckResult.Good

        if state in ["alive","initialized"]:
            return CheckResult.Pending
        
        if state in ["crashed", "crashed_init"]:
            return CheckResult.Crashed
        
        return CheckResult.Bad

class TaskNameDependency(LocalDependency):
    def __init__(self, task_name):
        self.task_name = task_name
    
    def check(self, run ,request):
        if self.task_name == request.task.name:
            return CheckResult.Good
        return CheckResult.Bad

class RunDependency(GlobalDependency):

    def __init__(self, child):
        self.child = child
    
    def check(self, run, _):
        return self.child.check_global_dependencies()


def _check_deps(deps, supertype, *args, **kwargs):
    ret = CheckResult.Good
    deps = [dep for dep in deps if not type(dep) is DoneDependency] + [dep for dep in deps if type(dep) is DoneDependency]
    for dep in deps:
        if issubclass(type(dep), supertype):
            res = dep.check(*args, **kwargs)
            if res is CheckResult.Bad:
                return CheckResult.Bad
            if res is CheckResult.Crashed:
                ret = CheckResult.Crashed
            if ret is CheckResult.Good and res is CheckResult.Pending:
                ret = CheckResult.Pending
    
    return ret

