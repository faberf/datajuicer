import itertools
import json, pickle, os


from itertools import product


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


def evaluate_network(network, data_loader, noise_level, seed):
    print(f"evaluating {network} on test data from {data_loader} with noise level {noise_level} and seed {seed}")
    return [1,2,3,4]


def train_network(network, data_loader, directory ,epochs, step_size, run_id, verbose):
        print(f"Training network {network} on the data provided by {data_loader} for {epochs} epochs with step size {step_size}")
        if verbose:
            print(f"continuously logging data in file train_{run_id}_data.json")
            print(f"saving model in file train_{run_id}_model.pickle")
        trained_net = thing(f"trained_{network}")
        
        with open(os.path.join(directory, f"train_{run_id}_model.pickle"), "wb+") as f:
            pickle.dump(trained_net,f)
        with open(os.path.join(directory, f"train_{run_id}_data.json"), "w+") as f:
            json.dump([1,2,3,4], f)

def find_misclassified_example(network, dataloader, noise_level):
    return thing(f"misclassified example")

#next we define the tasks that we will use. Train trains a network and Evaluate evaluates a network

import datajuicer as dj

DIRECTORY = "example1_data"


@dj.NoCache()
class MakeDataloader(dj.Task):
    name = "make_dataloader"

    def compute(datapoint, run_id):
        if datapoint["dataset"] == "mnist":
            return prepare_mnist(datapoint["data_directory"])
        
        if datapoint["dataset"] == "cifar":
            return prepare_cifar(datapoint["data_directory"])

    def get_dependencies(datapoint):
        return "dataset", "data_directory"

@dj.FileCache(("model.pickle", "data.json"), directory=DIRECTORY)
class Train(dj.Task):
    name="train"
    parents = (MakeDataloader,)
    
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
         datapoint["make_dataloader_output"],
         datapoint["data_directory"],
         datapoint["epochs"], 
         datapoint["step_size"], 
         run_id, 
         datapoint["verbose_training"])
    
    
    @staticmethod
    def get_dependencies(datapoint):
        dep = ["epochs", "step_size", "network", "dataset", "network_seed"]
        if datapoint["network"] == "mlp":
            dep += ["size", "layers"]
        if datapoint["network"] == "cnn":
            dep += ["filters", "dense_layers"]
        return dep

@dj.AutoCache(directory=DIRECTORY)
class Evaluate(dj.Task):

    name = "evaluate_v1"

    parents = (MakeDataloader, Train)

    @staticmethod
    def get_dependencies(datapoint):
        return ["noise_level", "train_run_id", "evaluation_seed"]

    @staticmethod
    def compute(datapoint, run_id):
        return evaluate_network(datapoint["train_output"]["model"], datapoint["make_dataloader_output"], datapoint["noise_level"], datapoint["evaluation_seed"])

@dj.NoCache()
class FindMisclassifiedExample(dj.Task):
    name = "misclassified_example"
    parents = (MakeDataloader, Train)

    @staticmethod
    def get_dependencies(datapoint):
        return ["noise_level", "train_run_id"]
    
    @staticmethod
    def compute(datapoint, run_id):
        return "cheese"

import numpy as np
metric_var = dj.ChainVariable(
        dj.JointVariable(
            dj.ReductionsVariable({
                "train_data": lambda data: Train.reduce(data).select("data"),
                "eval_data": Evaluate
            }),
            dj.ReductionsVariable(
                np.mean, 
                np.std
            )
        ),
        dj.ReductionsVariable(FindMisclassifiedExample)
) 

class Formatter(dj.BaseFormatter):

    def format_axis(self, variable):
        if variable == dj.Variable("dataset","network"):
            return "Task"
        if variable == dj.Variable("noise_level"):
            return "Noise"
                
        if variable == metric_var:
            return "Metric"
        

    def format_value(self, value, variable):
        if variable == dj.Variable("dataset","network"):
            dataset, network = value
            dataset = dataset.upper()
            network = network.upper()
            return f"{network} on {dataset}"
        
        if value == ('eval_data', 'std'):
            return "Eval Acc (std)"
        if value == ('train_data', 'std'):
            return "Train Acc (std)"
        if value == ('eval_data', 'mean'):
            return "Eval Acc (mean)"
        if value == ('train_data', 'mean'):
            return "Train Acc (mean)"

        return super().format_value(value, variable)
    
    def format_datapoint(self, datapoint):
        if type(datapoint) in [float, np.float64]:
            return "{:.2f}".format(datapoint)
        return super().format_datapoint(datapoint)

@dj.Table.make(3)
def simple_text_table(grid, title="Table", padding=1, align= "left", underline = False):
    
    column_widths = [max([len(s) for s in (grid.axis_names[2],) + grid.axes[2]]) + padding]
    for i, val in enumerate(grid.axes[1]):
        column_widths.append(
            max(
                [len(val) +  len(grid.axis_names[1]) + 3] +\
                [len(s) for s in grid[:,i,:].flatten()]
            ) + padding
        )
    
    
    
    line_len = sum(column_widths)
    def pad_field(s, column):
        offset = column_widths[column] - len(s) 
        if align == "left":
            return s + " " * offset
        if align == "right":
            return " " * offset + s
    def make_line(s):
        offset = line_len - len(s)
        return "\n|" + " "*padding + s + " " * offset + " "*padding + "|"
    
    divider = "\n|" + "-" * (line_len + 2*padding) + "|"
    top = "-" * (len(divider)-1)
    bottom = "\n" + top

    out = top
    out += make_line(title)
    out += divider
    for k in range(grid.shape[0]):
        out += divider
        out += make_line(grid.axes[0][k])
        if underline:
            out += make_line("-" * len(grid.axes[0][k]))
        line = pad_field(str(grid.axis_names[2]), 0)

        for i, val in enumerate(grid.axes[1]):
            line += pad_field(f"{grid.axis_names[1]} = {val}", i+1)
        
        out+= make_line(line)
        
        for i, val in enumerate(grid.axes[2]):
            line = pad_field(str(val), 0)
            for j in range(grid.shape[1]):
                line += pad_field(grid[k,j,i], j+1)
            out += make_line(line)

    return out + bottom
@dj.AutoCache(directory=DIRECTORY)
class NoiseExperiment(dj.Task):
    name = "noise_experiment"

    def compute(datapoint, run_id):
        
        models = dj.Frame().configure({"data_directory":datapoint["data_directory"], "step_size":datapoint["step_size"], "verbose_training":True})

        mlp = models.configure({"network": "mlp", "size" : datapoint["mlp_size"], "layers": datapoint["mlp_layers"]})
        cnn = models.configure({"network" : "cnn", "dense_layers":datapoint["cnn_dense_layers"], "filters":datapoint["cnn_filters"]})

        models = (mlp + cnn).vary("dataset", ["mnist", "cifar"])

        models = models.configure({"epochs":datapoint["mlp_mnist_epochs"]}, models.matches({"network":"mlp", "dataset":"mnist"}))
        models = models.configure({"epochs":datapoint["cnn_mnist_epochs"]}, models.matches({"network":"cnn", "dataset":"mnist"}))
        models = models.configure({"epochs":datapoint["mlp_cifar_epochs"]}, models.matches({"network":"mlp", "dataset":"cifar"}))
        models = models.configure({"epochs":datapoint["cnn_cifar_epochs"]}, models.matches({"network":"cnn", "dataset":"cifar"}))

        models = models.vary("noise_level", datapoint["noise_levels"])
        models = models.vary("network_seed", list(range(datapoint["training_repeats"])))
        models = models.vary("evaluation_seed", list(range(datapoint["evaluation_repeats"])))

        models = Evaluate.configure(models)
        models = Train.configure(models)
        models = FindMisclassifiedExample.configure(models)

        dj.ReductionsVariable(
                    lambda data: np.mean(data), 
                    lambda data: np.std(data)
                )

        vars = (dj.Variable("dataset","network"),dj.Variable("noise_level"),metric_var)

        return simple_text_table(models, vars, formatter=Formatter(), title="Noise Experiment")
    
    @staticmethod
    def run_default():
        default = {
            "data_directory":DIRECTORY,
            "mlp_size": 8,
            "mlp_layers": 2,
            "cnn_dense_layers": 2,
            "cnn_filters": [5,5],
            "mlp_mnist_epochs": 16,
            "cnn_mnist_epochs": 24,
            "mlp_cifar_epochs": 32,
            "cnn_cifar_epochs": 48,
            "step_size" : 0.1,
            "noise_levels": [1,2,3],
            "training_repeats": 3,
            "evaluation_repeats": 5
            }
        return NoiseExperiment.run(dj.Frame([default]), force=True)[0]





out = NoiseExperiment.run_default()

print(out)

Train.cache.clean_up()
Evaluate.cache.clean_up()
