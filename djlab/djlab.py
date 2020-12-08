from datajuicer import *
import subprocess
import re

def parse_arg_parse(file = "dnn.py"):
    result = subprocess.run(f"python {file} -h",shell=True, stdout=subprocess.PIPE)
    result = result.stdout.decode("utf-8")
    keys = re.findall(r'\[.*?\]', result)
    keys = [x[1:-1].split(None,1) for x in keys][1:]
    def cast(s,t):
        if t == "str":
            return str(s)
        if t == "float":
            return float(s)
        if t == "int":
            return int(s)
    
    vals = re.findall(r'\(default: .*?\)', result)
    vals = map(cast, [x[10:-1] for x in vals], [x[1].strip(" ") for x in keys])

    keys = [x[0].strip("-") for x in keys]
    return dict(zip(keys,vals))

class node():
    
    def __init__(self, pyfile, ):
        self.name = name
        self.output_keys = output_keys
        self.input_keys = input_keys
        self.defaults = defaults

    def make_callable(self):
        @cachable(
        dependencies=["model:"+key for key in self.input_keys], 
        saver = None,
        loader = None,
        checker= None,
        table_name=architecture.__name__
        )
        def runner(model):
            try:
                mode = get(model, "mode")
            except:
                mode = "direct"
            model["mode"] = mode
            model["args"] = " ".join([f"-{key}={get(model, key)}" for key in list(architecture.default_hyperparameters().keys())+env_vars + ["session_id"]])
            command = format_template(model,launch_settings[mode])
            os.system(command)
            return None

        return runner

class visual():
    x = "train_accuracy.0"
    y = "train_accuracy"
    z = ("size", [32, 64, 128])
    w = None

def make_tasks(nodes, visual):
    pass
    #returns list of (callable, dependency_list)