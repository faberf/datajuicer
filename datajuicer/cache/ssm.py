
import time

from datajuicer.cache.query import Query


class Uninitialized:
    pass

class SSM:

    def __init__(self, id, cache):
        self.id = id
        self.cache = cache
        self.lock = cache.get_lock(f"ssm_{id}")
    
    def _current_state(self):
        doc = self.cache.search(self.id)
        if not hasattr(doc, "__ssm__"):
            return Uninitialized
        ssm = doc["__ssm__"]
        if ssm["until"] is None or ssm["until"] >= time.time():
            return ssm["state"]
        return ssm["expired"]
    
    def current_state(self):
        with self.lock:
            return self._current_state()
    
    def transition(self, after_state, before_states=None, duration=None, expiration_state=None):
        with self.lock:
            if duration is None:
                until = None
            else:
                until = time.time() + duration
            if before_states is None or self._current_state() in before_states:
                self.cache.update(self.id, {"state":after_state, "until":until, "expired":expiration_state})

