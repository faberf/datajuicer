import datajuicer.utils as utils
import datajuicer.resource_lock as resource_lock
import datajuicer.requirements as requirements
from datajuicer.context import Context

class SessionMode(requirements.Any):
    def __init__(self):
        super().__init__(self)

class NewSession(SessionMode):
    def __init__(self, max_workers, **resources):
        super().__init__()
        self.max_workers = max_workers
        self.resources = resources
    
    def make_resource_lock(self, scratch=None):
        if scratch is None:
            scratch = Context.get_active().scratch
        rl = resource_lock.ResourceLock(utils.rand_id(), scratch)
        for _ in range(self.max_workers):
            rl.release()
        rl.free_global_resources(**self.resources)
        return rl

class Attach(SessionMode):
    def __init__(self):
        super().__init__()
    def make_resource_lock(self):
        return Context.get_active().resource_lock