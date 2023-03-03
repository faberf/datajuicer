from contextlib import redirect_stderr, redirect_stdout
import threading
import sys


class Proxy(object):
    def __init__(self, default):
        self.default = default                 
        self.loggers={}                                 

    def register(self, logger):                    
        ident = threading.currentThread().ident         
        self.loggers[ident] = self.loggers.get(ident, [self.default]) + [logger]
    
    def deregister(self):
        ident = threading.currentThread().ident
        self.loggers[ident].pop()

    def getlogger(self):
        ident = threading.currentThread().ident 
        return self.loggers.get(ident, [self.default])[-1]
    
    def write(self, message):            
        self.getlogger().write(message)
        
    def flush(self):
        self.getlogger().flush()
    
    def getvalue(self):
        return self.getlogger().getvalue()

class Redirect:
    def __init__(self, log_file):
        self.log_file = log_file
    def __enter__(self):
        self.log_file.__enter__()
        
        
        if not hasattr(sys.stdout, "register"):
            sys.stdout = Proxy(sys.stdout)
        if not hasattr(sys.stderr, "register"):
            sys.stderr = Proxy(sys.stderr)

        outlogger = Logger(self.log_file, console = sys.stdout.getlogger())
        errlogger = Logger(self.log_file, console=sys.stderr.getlogger())
        
        sys.stdout.register(outlogger)
        sys.stderr.register(errlogger)

        # self.redirect_stdout = redirect_stdout(outlogger)
        # self.redirect_stderr = redirect_stderr(errlogger)
        # self.redirect_stdout.__enter__()
        # self.redirect_stderr.__enter__()
        
        return self
    
    def __exit__(self, exc_type, exc_value, exc_traceback):

        # self.redirect_stderr.__exit__(exc_type, exc_value, exc_traceback)
        # self.redirect_stdout.__exit__(exc_type, exc_value, exc_traceback)
        
        sys.stdout.deregister()
        sys.stderr.deregister()
        self.log_file.__exit__(exc_type, exc_value, exc_traceback)


class Logger:
     
    def __init__(self, file, console, mute = False):
        self.console = console
        self.file = file
        self.mute= mute
 
    def write(self, message):
        if not self.mute:
            self.console.write(message)
        self.file.write(message)
 
    def flush(self):
        if not self.mute:
            self.console.flush()
        self.file.flush()
    
    def close(self):
        return self.file.close()
