
import threading
import subprocess
from datajuicer.requirements import Any

ALIVE_COOLDOWN = 10.0
TICK_EVERY = 2.0

DIRECT_INIT_TIME = 0.1
THREAD_INIT_TIME = 1.0
PROCESS_INIT_TIME = 200.0
COMMAND_INIT_TIME = 200.0


class Launcher(Any):

    def __init__(self, init_cooldown, alive_cooldown, tick_every):
        super().__init__(self)
        self.init_cooldown = init_cooldown
        self.alive_cooldown = alive_cooldown
        self.tick_every =  tick_every
    
    def launch(self, context):
        pass






class Direct(Launcher):
    def __init__(self, init_cooldown = DIRECT_INIT_TIME, alive_cooldown = ALIVE_COOLDOWN, tick_every=TICK_EVERY):
        super().__init__(init_cooldown, alive_cooldown, tick_every)
    
    def launch(self, context):
        from datajuicer.context import Context
        Context.get_active().resource_lock.release()
        context.execute()
        Context.get_active().resource_lock.acquire()

class NewThread(Launcher):

    class Thread(threading.Thread):
        def __init__(self, context):
            self.context = context
        def run(self):
            self.context.execute()
    
    def __init__(self, init_cooldown = THREAD_INIT_TIME, alive_cooldown = ALIVE_COOLDOWN, tick_every = TICK_EVERY):
        super().__init__(init_cooldown, alive_cooldown, tick_every)

    def launch(self, context):
        NewThread.Thread(context).run()

class NewProcess(Launcher):

    def __init__(self, init_cooldown = PROCESS_INIT_TIME, alive_cooldown = ALIVE_COOLDOWN, tick_every = TICK_EVERY):
        super().__init__(init_cooldown, alive_cooldown, tick_every)

    def launch(self, context):
        context.to_disk()
        command = context.get_command()
        subprocess.Popen(command.split())


class Command(Launcher):

    def __init__(self, template, init_cooldown = COMMAND_INIT_TIME, alive_cooldown = ALIVE_COOLDOWN, tick_every = TICK_EVERY):
        super().__init__(init_cooldown, alive_cooldown, tick_every)
        self.template = template

    def launch(self, context):
        context.to_disk()
        command = context.get_command()
        command = self.template.replace("COMMAND", command)
        subprocess.Popen(command,shell=True)
