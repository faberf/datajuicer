
import time

from datajuicer.cache.query import Query


class Uninitialized:
    pass

class SSM:
    """Synchronized State Machine. This class is used to implement state machines that are synchronized across multiple processes. It is used to implement the state machine of a task.
    """

    def __init__(self, id, cache):
        """Create a new SSM.

        Args:
            id (str): id of the SSM.
            cache (Cache): Cache where the SSM is stored.
        """
        self.id = id
        self.cache = cache
        self.lock = cache.get_lock(f"ssm_{id}")
    
    def _current_state(self):
        doc = self.cache.search(self.id)
        if not "__ssm__" in doc:  #TODO: hasattr or has item?
            return Uninitialized
        ssm = doc["__ssm__"]
        if ssm["until"] is None or ssm["until"] >= time.time():
            return ssm["state"]
        return ssm["expired"]
    
    def current_state(self):
        """Get the current state of the SSM.

        Returns:
            state (object): the current state.
        """
        with self.lock:
            return self._current_state()
    
    def transition(self, after_state, before_states=None, duration=None, expiration_state=None):
        """Atomically transition the SSM to a new state.

        Args:
            after_state (object): The state to transition to.
            before_states (collection, optional): The states that the SSM must be in to transition. Defaults to None implying any state.
            duration (float, optional): How long the SSM should stay in the new state in seconds. Defaults to None implying forever.
            expiration_state (object, optional): The state to transition to when the duration expires. Defaults to None implying the same state.
        """        
        with self.lock:
            if duration is None:
                until = None
            else:
                until = time.time() + duration
            if before_states is None or self._current_state() in before_states:
                self.cache.update(self.id, {"__ssm__":{"state":after_state, "until":until, "expired":expiration_state}})

