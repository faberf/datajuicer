from contextlib import redirect_stderr, redirect_stdout
import sys


class Redirect:
    def __init__(self, log_file):
        self.log_file = log_file
    def __enter__(self):
        self.log_file.__enter__()

        outlogger = Logger(self.log_file)
        errlogger = Logger(self.log_file, console="stderr")

        self.redirect_stdout = redirect_stdout(outlogger)
        self.redirect_stderr = redirect_stderr(errlogger)
        self.redirect_stdout.__enter__()
        self.redirect_stderr.__enter__()
    
    def __exit__(self, exc_type, exc_value, exc_traceback):

        self.redirect_stderr.__exit__(exc_type, exc_value, exc_traceback)
        self.redirect_stdout.__exit__(exc_type, exc_value, exc_traceback)
        self.log_file.__exit__(exc_type, exc_value, exc_traceback)


class Logger:
     
    def __init__(self, file, mute = False, console="stdout"):
        self.console = getattr(sys,console)
        self.file = file
        self.mute= mute
 
    def write(self, message):
        if not self.mute:
            self.console.write(message)
        self.file.write(message)
 
    def flush(self):
        self.console.flush()
        self.file.flush()