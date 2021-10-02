import json, pickle, os

###Here we define some dummy functions that perform useful machine learning tasks

class thing:
    def __init__(self, name):
        self.name = name
    
    def __str__(self):
        return self.name

def prepare_mnist(directory):
    print(f"preparing mnist at location {directory} if not already there and returning dataloader")
    return thing(f"mnist_dataloader_at_{directory}")

def prepare_cifar(directory):
    print(f"preparing cifar at location {directory} if not already there and returning dataloader")
    return thing(f"cifar_dataloader_at_{directory}")

def prepare_mlp(size, layers, input, output, seed):
    print(f"made an mlp of size {size} with {layers} hidden layers, input shape {input} and output shape {output} (seed {seed})")
    return thing(f"mlp_{size}_{layers}_{input}_{output}")

def prepare_cnn(filters, dense_layers, input, output, seed):
    print(f"made a CNN consisting of the filters: {filters} and {dense_layers} dense layers. It has input shape {input} and output shape {output} (seed {seed})")
    return thing(f"cnn_{filters}_{dense_layers}_{input}_{output}_{seed}")


def evaluate_network(network, data_loader, noise_level):
    print(f"evaluating {network} on test data from {data_loader} with noise level {noise_level}")
    return thing("evaluation_data")


def train_network(network, data_loader, directory ,epochs, step_size, run_id, verbose):
        print(f"Training network {network} on the data provided by {data_loader} for {epochs} epochs with step size {step_size}")
        if verbose:
            print(f"continuously logging data in file {run_id}_data.json")
            print(f"saving model in file {run_id}_model.pickle")
        trained_net = thing(f"trained_{network}")
        
        with open(os.path.join(directory, f"{run_id}_model.pickle"), "wb+") as f:
            pickle.dump(trained_net,f)
        with open(os.path.join(directory, f"{run_id}_data.json"), "w+") as f:
            json.dump([1,2,3,4], f)

#next we define the tasks that we will use. Train trains a network and Evaluate evaluates a network

import datajuicer as dj

def configure_unique_dataloaders(data):
    dataset = dj.Switch(dj.select(data, "dataset"))

    data_dirs = dj.select(data, "data_directory")

    unique_mnist_dirs = dj.Unique(dataset.case(data_dirs, "mnist"))
    unique_cifar_dirs = dj.Unique(dataset.case(data_dirs, "cifar"))

    dataloaders = dataset.join({
        "mnist" : unique_mnist_dirs.expand(dj.run(prepare_mnist, unique_mnist_dirs.data)),
        "cifar" : unique_cifar_dirs.expand(dj.run(prepare_cifar, unique_cifar_dirs.data))
    })

    return dj.configure(data, {"data_loader":dataloaders})

    

class Train(dj.Task):
    name="train"

    @staticmethod
    def preprocess(data):
        return configure_unique_dataloaders(data)
    
    @staticmethod
    def compute(datapoint, run_id):

        if datapoint["dataset"] == "mnist":
            input_shape = (25,25)
            output_shape = (10,)
        if datapoint["dataset"] == "cifar":
            input_shape = (50,50)
            output_shape = (100,)

        #prepare_network
        if datapoint["network"] == "mlp":
            network = prepare_mlp(datapoint["size"], datapoint["layers"], input_shape, output_shape, datapoint["network_seed"])
        if datapoint["network"] == "cnn":
            network = prepare_cnn(datapoint["filters"], datapoint["dense_layers"], input_shape, output_shape, datapoint["network_seed"])
        
        #train_network
        train_network(network,
         datapoint["data_loader"],
         datapoint["data_directory"],
         datapoint["epochs"], 
         datapoint["step_size"], 
         run_id, 
         datapoint["verbose_training"])
    
    @staticmethod
    def check(datapoint, run_id):
        directory = datapoint["data_directory"]
        return os.path.isfile(os.path.join(directory, f"{run_id}_model.pickle")) and os.path.isfile(os.path.join(directory, f"{run_id}_data.json"))
    
    @staticmethod
    def get_dependencies(datapoint):
        dep = ["epochs", "step_size", "network", "dataset", "network_seed"]
        if datapoint["network"] == "mlp":
            dep += ["size", "layers"]
        if datapoint["network"] == "cnn":
            dep += ["filters", "dense_layers"]
        return dep
    
    @staticmethod
    def load(datapoint, run_id):
        directory = datapoint["data_directory"]
        with open(os.path.join(directory, f"{run_id}_model.pickle"), "rb") as f:
            model = pickle.load(f)
        with open(os.path.join(directory, f"{run_id}_data.json"), "r") as f:
            data = json.load(f)
        
        return {"trained_model":model, "training_data":data}

class Evaluate(dj.Task):

    name = "evaluate"

    parents = [Train]

    @staticmethod
    def preprocess(data):
        return configure_unique_dataloaders(data)

    @staticmethod
    def get_dependencies(datapoint):
        return ["noise_level"]

    @staticmethod
    def compute(datapoint, run_id):
        with open(os.path.join(datapoint["eval_dir"], f"{run_id}_eval_data.pickle"), "wb+") as f:
            eval_data = evaluate_network(datapoint["train_output"]["trained_model"], datapoint["data_loader"], datapoint["noise_level"])
            pickle.dump(eval_data,f)

    @staticmethod
    def check(datapoint, run_id):
        directory = datapoint["eval_dir"]
        return os.path.isfile(os.path.join(directory, f"{run_id}_eval_data.pickle")) 

    @staticmethod
    def load(datapoint, run_id):
        directory = datapoint["eval_dir"]
        with open(os.path.join(directory, f"{run_id}_eval_data.pickle"), "rb") as f:
            return pickle.load(f)

from itertools import product

class Function:
    class DependentVar:
        def __init__(self, task, key, reduction=None):
            self.task = task
            self.key = key

            if not reduction:
                self.reduction = lambda x: x
    
    def __init__(self, independent_vars, dependent_vars):
        
        self.independent_vars = independent_vars
        self.dependent_vars = dependent_vars
    
    def run(self, data, directory=".", force=False, incognito=False, n_threads=1):

        for dep_var in self.dependent_vars:
            data = dep_var.task.configure(data, directory, force, incognito, n_threads)
        
        ret = {}

        all_indep_vals = [dj.Unique(dj.select(data, indep_var)).data for indep_var in self.independent_vars]

        for indep_vals in product(all_indep_vals):
            for dep_var in self.dependent_vars:
                key = tuple(indep_vals + [dep_var.task.name + "_" + dep_var.key] )
                matches_dict = {key:indep_vals[i] for (i,key) in enumerate(self.independent_vars)}
                matching = dj.Where(dj.matches(data, matches_dict)).true(data)
                task_outputs = dj.select(matching, dep_var.task.name + "_" + "output")
                task_rids = dj.select(matching, dep_var.task.name + "_" + "run_id")
                task_outputs_unique = dj.Where(dj.Unique(task_rids).is_canonical).true(task_outputs)
                ret[key] = dj.select(task_outputs_unique, dep_var.key)
        
        return ret



        
directory = "example1_data"

models = dj.configure(dj.Frame.new(), {"data_directory":directory, "eval_dir":directory, "step_size":0.01, "verbose_training":True})

mlp = dj.configure(models, {"network": "mlp"})
cnn = dj.configure(models, {"network" : "cnn"})

mlp = dj.vary(mlp, "size", [4,8,16])
mlp = dj.vary(mlp, "layers", [1,2])

cnn = dj.vary(cnn, "dense_layers", [1,2])
cnn = dj.vary(cnn, "filters", [[5,5], [10,10]])

models = dj.vary(mlp + cnn, "dataset", ["mnist", "cifar"])

models = dj.configure(models, {"epochs":16}, [model["network"] == "mlp" and model["dataset"] == "mnist" for model in models])
models = dj.configure(models, {"epochs":24}, [model["network"] == "cnn" and model["dataset"] == "mnist" for model in models])
models = dj.configure(models, {"epochs":32}, [model["network"] == "mlp" and model["dataset"] == "cifar" for model in models])
models = dj.configure(models, {"epochs":48}, [model["network"] == "cnn" and model["dataset"] == "cifar" for model in models])

models = dj.vary(models, "epochs", dj.Frame([[int(epochs *0.5), epochs, epochs*2] for epochs in dj.select(models, "epochs")]))

models = dj.vary(models, "noise_level", [1,2])

models = dj.vary(models, "network_seed", [0,1,2,3])

database = dj.FastSQLiteDB(directory)
out = Evaluate.run(models, database)

#indep_vars = []






def clean_up(database, dir):
    files = [f for f in os.listdir(dir) if os.path.isfile(os.path.join(dir, f))]

    all_ids = database.get_all_runs("train")

    for f in files:
        if not f[0:10] in all_ids and (f.endswith("_model.pickle") or f.endswith("_data.json")):
            os.remove(os.path.join(dir,f))
    
    def bad_id(run_id):
        return not (os.path.isfile(os.path.join(dir, f"{run_id}_model.pickle")) and os.path.isfile(os.path.join(dir, f"{run_id}_data.json")))
    
    database.delete_runs("train",[rid for rid in all_ids if bad_id(rid)])

clean_up(database, directory)
