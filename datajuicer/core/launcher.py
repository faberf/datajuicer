import subprocess
import threading

ALIVE_COOLDOWN = 10.0
TICK_EVERY = 2.0

DIRECT_PENDING_TIME = 0.1
THREAD_PENDING_TIME = 1.0
PROCESS_PENDING_TIME = 200.0
JOB_PENDING_TIME = 200.0


"""This module contains the Launcher class and its subclasses. They are used to launch executions in different ways.
"""

class Launcher:
    def __init__(self, pending_cooldown, alive_cooldown, tick_every):
        self.pending_cooldown = pending_cooldown
        self.alive_cooldown = alive_cooldown
        self.tick_every = tick_every
    
    def launch(self, execution):
        raise Exception

class Direct(Launcher):
    """ Launches the execution directly.
    """

    def __init__(
        self, 
        pending_cooldown= DIRECT_PENDING_TIME, 
        alive_cooldown = ALIVE_COOLDOWN, 
        tick_every = TICK_EVERY
        ):
        super().__init__(pending_cooldown=pending_cooldown, alive_cooldown=alive_cooldown, tick_every=tick_every)

    def launch(self, execution):
        execution.execute()

class NewThread(Launcher):
    """Launches the execution in a new thread."""
    def __init__(
        self, 
        pending_cooldown = THREAD_PENDING_TIME, 
        alive_cooldown = ALIVE_COOLDOWN, 
        tick_every = TICK_EVERY
        ):
        super().__init__(pending_cooldown=pending_cooldown, alive_cooldown=alive_cooldown, tick_every=tick_every)

    def launch(self, execution):
        threading.Thread(target = execution.execute).start()

class NewProcess(Launcher):
    """Launches the execution in a new process."""
    def __init__(
        self, 
        pending_cooldown = PROCESS_PENDING_TIME, 
        alive_cooldown = ALIVE_COOLDOWN, 
        tick_every = TICK_EVERY
        ):
        super().__init__(pending_cooldown=pending_cooldown, alive_cooldown=alive_cooldown, tick_every=tick_every)

    def launch(self, execution):
        command = execution.make_job()
        subprocess.Popen(command.split())


class NewJob(Launcher):
    """Launches the execution in a new job. The job is defined by a template that contains the string "COMMAND" which will be replaced by the command to execute the run."""

    def __init__(
        self, 
        template, 
        pending_cooldown = JOB_PENDING_TIME, 
        alive_cooldown = ALIVE_COOLDOWN, 
        tick_every = TICK_EVERY
        ):
        super().__init__(pending_cooldown, alive_cooldown, tick_every)
        self.template = template

    def launch(self, execution):
        command = execution.make_job()
        command = self.template.replace("COMMAND", command)
        subprocess.Popen(command,shell=True)
