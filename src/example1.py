import itertools
import json, pickle, os


from itertools import product

from datajuicer.task import Ignore


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

def train_network(network, data_loader ,epochs, step_size, verbose):
        print(f"Training network {network} on the data provided by {data_loader} for {epochs} epochs with step size {step_size}")
        if verbose:
            print(f"continuously logging data in file data.json")
            print(f"saving model in file model.pickle")
        trained_net = thing(f"trained_{network}")
        
        with dj.open("model.pickle", "wb+") as f:
            pickle.dump(trained_net,f)
        with dj.open("data.json", "w+") as f:
            json.dump([1,2,3,4], f)

def find_misclassified_example(network, dataloader, noise_level):
    return thing(f"misclassified example")


#next we define the tasks that we will use. Train trains a network and Evaluate evaluates a network

import datajuicer as dj

DIRECTORY = "example1_data"

@dj.Task.make(cache=dj.Temporary(), directory = dj.Ignore)
def make_dataloader(dataset, directory = DIRECTORY):
    if dataset == "mnist":
        return prepare_mnist(directory)
    if dataset == "cifar":
        return prepare_cifar(directory)

input_dim = {"cifar" : (50,50), "mlp":(25,25)}
output_dim = {"cifar" : (100,), "mlp":(10,)}


@dj.Task.make(verbose = dj.Ignore)
def train(architecture, epochs, dataset, step_size, verbose, hyperparams):
    dataloader = make_dataloader(dataset)
    if epochs > 1:
        with train(architecture, epochs - 1, dataset, **hyperparams).open("model.pickle", "rb") as f:
            network = pickle.load(f)
    elif "architecture" == "mlp":
        network = prepare_mlp(hyperparams["size"], hyperparams["layers"], input_dim[dataset], output_dim[dataset], hyperparams["seed"])
    elif "architecture" == "cnn":
        network = prepare_cnn(hyperparams["filters"], hyperparams["dense_layers"], input_dim[dataset], output_dim[dataset], hyperparams["seed"])
    train_network(network, dataloader, 1, step_size, verbose)


@dj.Task.make()
def evaluate(train_run, noise_level, seed):
    with train_run.open("model.pickle", "rb") as f:
        network = pickle.load(f)
    dataloader = make_dataloader(train_run["dataset"])
    return evaluate_network(network, dataloader, noise_level, seed)


@dj.Task.make()
def find_misclassified(train_run, noise_level):
    with train_run.open("model.pickle", "rb") as f:
        network = pickle.load(f)
    dataloader = make_dataloader(train_run["dataset"])
    return find_misclassified_example(network, dataloader, noise_level)
    

def noise_experiment(configuration):
        models = dj.Frame().configure({"step_size":configuration["step_size"], "verbose_training":True})

        models.vary("architecture_seed", list(range(configuration["training_repeats"])))

        mlp = models.copy().configure({"architecture": "mlp", "hyperparams":{"size" : configuration["mlp_size"], "layers": configuration["mlp_layers"], "seed":models.select("architecture_seed")}})

        cnn = models.copy().configure({"architecture" : "cnn", "hyperparams":{"dense_layers":configuration["cnn_dense_layers"], "filters":configuration["cnn_filters"], "seed":models.select("architecture_seed")}})

        models = (mlp + cnn).vary("dataset", ["mnist", "cifar"])

        models.configure({"epochs":configuration["mlp_mnist_epochs"]}, models.matches({"architecture":"mlp", "dataset":"mnist"}))
        models.configure({"epochs":configuration["cnn_mnist_epochs"]}, models.matches({"architecture":"cnn", "dataset":"mnist"}))
        models.configure({"epochs":configuration["mlp_cifar_epochs"]}, models.matches({"architecture":"mlp", "dataset":"cifar"}))
        models.configure({"epochs":configuration["cnn_cifar_epochs"]}, models.matches({"architecture":"cnn", "dataset":"cifar"}))

        models.vary("noise_level", configuration["noise_levels"])
        models.vary("evaluation_seed", list(range(configuration["evaluation_repeats"])))



        models.configure(
            {"train_acc": train(models.select("architecture"), models.select("epochs"), models.select("dataset"), configuration["step_size"], configuration["verbose"], models.select("hyperparams"))}
            )

        models = Evaluate.configure(models)
        models = Train.configure(models)
        models = FindMisclassifiedExample.configure(models)

# @dj.NoCache()
# class MakeDataloader(dj.Task):
#     name = "make_dataloader"

#     def compute(datapoint, run_id):
#         if datapoint["dataset"] == "mnist":
#             return prepare_mnist(datapoint["data_directory"])
        
#         if datapoint["dataset"] == "cifar":
#             return prepare_cifar(datapoint["data_directory"])

#     def get_dependencies(datapoint):
#         return "dataset", "data_directory"

# @dj.FileCache(("model.pickle", "data.json"), directory=DIRECTORY)
# class Train(dj.Task):
#     name="train"
#     parents = (MakeDataloader,)
    
#     @staticmethod
#     def compute(datapoint, run_id):

#         if datapoint["dataset"] == "mnist":
#             input_shape = (25,25)
#             output_shape = (10,)
#         if datapoint["dataset"] == "cifar":
#             input_shape = (50,50)
#             output_shape = (100,)

#         #prepare_network
#         if datapoint["network"] == "mlp":
#             network = prepare_mlp(datapoint["size"], datapoint["layers"], input_shape, output_shape, datapoint["network_seed"])
#         if datapoint["network"] == "cnn":
#             network = prepare_cnn(datapoint["filters"], datapoint["dense_layers"], input_shape, output_shape, datapoint["network_seed"])
        
#         #train_network
#         train_network(network,
#          datapoint["make_dataloader_output"],
#          datapoint["data_directory"],
#          datapoint["epochs"], 
#          datapoint["step_size"], 
#          run_id, 
#          datapoint["verbose_training"])
    
    
#     @staticmethod
#     def get_dependencies(datapoint):
#         dep = ["epochs", "step_size", "network", "dataset", "network_seed"]
#         if datapoint["network"] == "mlp":
#             dep += ["size", "layers"]
#         if datapoint["network"] == "cnn":
#             dep += ["filters", "dense_layers"]
#         return dep

# @dj.AutoCache(directory=DIRECTORY)
# class Evaluate(dj.Task):

#     name = "evaluate_v1"

#     parents = (MakeDataloader, Train)

#     @staticmethod
#     def get_dependencies(datapoint):
#         return ["noise_level", "train_run_id", "evaluation_seed"]

#     @staticmethod
#     def compute(datapoint, run_id):
#         return evaluate_network(datapoint["train_output"]["model"], datapoint["make_dataloader_output"], datapoint["noise_level"], datapoint["evaluation_seed"])

# @dj.NoCache()
# class FindMisclassifiedExample(dj.Task):
#     name = "misclassified_example"
#     parents = (MakeDataloader, Train)

#     @staticmethod
#     def get_dependencies(datapoint):
#         return ["noise_level", "train_run_id"]
    
#     @staticmethod
#     def compute(datapoint, run_id):
#         return "cheese"

# @dj.AutoCache(directory=DIRECTORY)
# class NoiseExperiment(dj.Task):
#     name = "noise_experiment"

#     def compute(datapoint, run_id):
        
#         models = dj.Frame().configure({"data_directory":datapoint["data_directory"], "step_size":datapoint["step_size"], "verbose_training":True})

#         mlp = models.configure({"network": "mlp", "size" : datapoint["mlp_size"], "layers": datapoint["mlp_layers"]})
#         cnn = models.configure({"network" : "cnn", "dense_layers":datapoint["cnn_dense_layers"], "filters":datapoint["cnn_filters"]})

#         models = (mlp + cnn).vary("dataset", ["mnist", "cifar"])

#         models = models.configure({"epochs":datapoint["mlp_mnist_epochs"]}, models.matches({"network":"mlp", "dataset":"mnist"}))
#         models = models.configure({"epochs":datapoint["cnn_mnist_epochs"]}, models.matches({"network":"cnn", "dataset":"mnist"}))
#         models = models.configure({"epochs":datapoint["mlp_cifar_epochs"]}, models.matches({"network":"mlp", "dataset":"cifar"}))
#         models = models.configure({"epochs":datapoint["cnn_cifar_epochs"]}, models.matches({"network":"cnn", "dataset":"cifar"}))

#         models = models.vary("noise_level", datapoint["noise_levels"])
#         models = models.vary("network_seed", list(range(datapoint["training_repeats"])))
#         models = models.vary("evaluation_seed", list(range(datapoint["evaluation_repeats"])))

#         models = Evaluate.configure(models)
#         models = Train.configure(models)
#         models = FindMisclassifiedExample.configure(models)
    
#     @staticmethod
#     def run_default():
#         default = {
#             "data_directory":DIRECTORY,
#             "mlp_size": 8,
#             "mlp_layers": 2,
#             "cnn_dense_layers": 2,
#             "cnn_filters": [5,5],
#             "mlp_mnist_epochs": 16,
#             "cnn_mnist_epochs": 24,
#             "mlp_cifar_epochs": 32,
#             "cnn_cifar_epochs": 48,
#             "step_size" : 0.1,
#             "noise_levels": [1,2,3],
#             "training_repeats": 3,
#             "evaluation_repeats": 5
#             }
#         return NoiseExperiment.run(dj.Frame([default]), force=True)[0]





out = NoiseExperiment.run_default()

print(out)

Train.cache.clean_up()
Evaluate.cache.clean_up()
