import os
from datajuicer.cache.equality_query import Matches
from datajuicer.cache.ssm import SSM, Uninitialized
from datajuicer.errors import NoResultException
import dill as pickle
from datajuicer.core.runstate import RunState

class RunState:
    class Pending:
        pass
    class Abandoned:
        pass
    class NotResponding:
        pass
    class Complete:
        pass
    class Exception:
        pass
    class Alive:
        pass

class Run:

    def __init__(self, run_id, cache, function):
        self.run_id = run_id
        self.cache = cache
        self.function = function
        self.runstate = SSM(self.run_id, self.cache)
    
    def get_directory(self):
        return os.path.join(self.cache.directory, self.run_id)
    
    def get_state(self):
        return self.runstate.current_state()

    def open(self, path, mode):
        return open(os.path.join(self.get_directory(), path), mode)

    def get_log(self):
        with self.open("log.txt", "r") as f:
            return f.read()
    
    def get_result(self):
        if self.get_state() is RunState.Complete:
            with self.open("result.pickle", "rb") as f:
                return pickle.load(f)
        if self.get_state() is RunState.Exception:
            with self.open("exception.pickle", "rb") as f:
                raise pickle.load(f)
        else:
            raise NoResultException
    
    def delete(self):
        self.cache.delete(Matches(dict(id = self.run_id)))
    
    def record_complete(self, result):
        self.runstate.transition(before_states=None, after_state=RunState.Complete, duration=None, expiration_state=None)
        with self.open("result.pickle", "wb+") as f:
            pickle.dump(result, f)
    
    def record_exception(self, exception):
        self.runstate.transition(before_states=None, after_state=RunState.Exception, duration=None, expiration_state=None)

        with self.open("exception.pickle", "wb+") as f:
            pickle.dump(exception, f)

    def record_alive(self, cooldown):
        self.runstate.transition(
            before_states=[RunState.NotResponding, RunState.Alive, RunState.Pending], 
            after_state=RunState.Alive, duration=cooldown, 
            expiration_state=RunState.NotResponding
        )
    
    def record_pending(self, cooldown):
        self.runstate.transition(
            before_states=[Uninitialized, RunState.Pending], 
            after_state=RunState.Pending, 
            duration=cooldown, 
            expiration_state=RunState.Abandoned
            )