from datajuicer.context import Context
from datajuicer.dependency import CheckResult
from datajuicer.launcher import Direct
from datajuicer.session_mode import Attach
from datajuicer.run import Run
from datajuicer.frame import take_frames, Frame
import datajuicer.dependency as dependency

class Handler:

    def handle(self, request):
        return None

    def __call__(self, *args, **kwargs):
        task = args[0]
        args = args[1:]
        return self.handle(task.request(*args, **kwargs))
        
class ForceHandler(Handler):

    def __init__(self, launcher, session_mode, incognito):
        self.launcher = launcher
        self.session_mode = session_mode
        self.incognito = incognito


    @take_frames
    def handle(self, request):
        resource_lock = self.session_mode.make_resource_lock()
        con = Context.get_active()

        child_run = Run.make(con.run.cache, self.incognito, request.task.name, request.parameters, self.launcher.init_cooldown)
        child_context = Context(
            run = child_run,
            resource_lock = resource_lock,
            scratch_space = con.scratch,
            incognito=self.incognito,
            lookup=con.lookup,
            request= request,
            tick_every = self.launcher.tick_every,
            alive_cooldown=self.launcher.alive_cooldown
        )
        self.launcher.launch(child_context)

        return child_run

class FindHandler(Handler):
    def __init__(self, find_good=True, find_pending=True, find_crashed=False, use_lookup=False):
        acceptable = []
        if find_good:
            acceptable.append(CheckResult.Good)
        if find_pending:
            acceptable.append(CheckResult.Pending)
        if find_crashed:
            acceptable.append(CheckResult.Crashed)
        self.acceptable = acceptable
        self.use_lookup = use_lookup
    
    @take_frames
    def handle(self, request):
        
        ac  = Context.get_active()
        if self.use_lookup:
            lurid = ac.lookup.lookup(request)
            if not lurid is None and ac.run.cache.exists(lurid):
                run = Run(lurid, ac.run.cache)
                if run.check_dependencies(request) in self.acceptable:
                    return run
        
        if not hasattr(ac, "table"):
            ac = Context.get_active()
            ac.table = []
            all_runs = ac.all_runs()
            for i,run in enumerate(all_runs):
                ac.table.append((run, run.load_deps()))
        
        for run, deps in ac.table:
            if dependency._check_deps(deps, object, run, request) in self.acceptable:
                if self.use_lookup:
                    ac.lookup.remember(request, run)
                return run

class ChainHandler(Handler):
    def __init__(self, *handlers):
        self.handlers = handlers
    
    @take_frames
    def handle(self, request):
        for handler in self.handlers:
            ret = handler.handle(request)
            if not ret is None:
                return ret

        # retf = Frame()
        # retf["req"] = request
        # retf["run"] = None
        # for handler in self.handlers:
        #     retf.where(retf["run"] == None)["run"] = handler.handle(retf["req"].where(retf["run"] == None))
        # return retf["run"]
        

class StandardHandler(ChainHandler):

    def __init__(self, force=False, incognito=False, session_mode=Attach(), launcher=Direct()):
        self.force = force
        fh = ForceHandler(launcher=launcher, session_mode=session_mode, incognito=incognito)
        if not force:
            super().__init__(FindHandler(find_good = True, find_pending=True, find_crashed=False, use_lookup=True), fh)
        else:
            super().__init__(fh)