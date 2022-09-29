
from datajuicer import requirements
from datajuicer import launcher
import datajuicer.frame as frame
from datajuicer.handler import StandardHandler
from datajuicer.request import Request
import importlib, pathlib, inspect
import inspect
from datajuicer.launcher import Direct
from datajuicer.session_mode import Attach
from datajuicer.function import Function
from datajuicer.frame import Frame, take_frames
import copy




class Task(Function):
    def __init__(self, func):
        self.name = func.__name__
        super().__init__(func)
    
    @frame.take_frames
    def request(self, *args, **kwargs):
        parameters = self.apply_defaults(*args, **kwargs)
        return Request(self,parameters)
    
    @frame.take_frames
    def __call__(self, *args, **kwargs):
        force = kwargs.pop("force", False)
        incognito = kwargs.pop("incognito", False)
        launcher = kwargs.pop("launcher", Direct())
        session_mode = kwargs.pop("session_mode", Attach())
        req = self.request(*args, **kwargs)
        handler = StandardHandler(force=force, incognito = incognito, session_mode = session_mode, launcher=launcher)
        return handler.handle(req)
