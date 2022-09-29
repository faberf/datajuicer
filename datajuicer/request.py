from datajuicer.hash import get_hash
import datajuicer.requirements as requirements

class Request:

    def __init__(
        self,
        task,
        parameters,
    ):
        self.task = task
        self.parameters = parameters
    
    def execute(self):
        return self.task.get_func()(**requirements.extract(self.parameters))
    
    def __hash__(self):
        return get_hash([self.task.name, self.parameters])
    



