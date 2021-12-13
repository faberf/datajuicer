import torch
from torch import nn
from torch.utils.data import DataLoader
from torchvision import datasets
from torchvision.transforms import ToTensor, Lambda, Compose
import matplotlib.pyplot as plt
import datajuicer as dj
from defaults import defaults
import copy
from sklearn.metrics import f1_score, classification_report, confusion_matrix, accuracy_score
import random
import numpy as np
import pandas as pd
#import datapane as dp
import matplotlib.pyplot as plt

if __name__ == "__main__":
    dj.setup(max_workers=3, clean=True)
    #dj.free_resources(gpus=3)

training_data = datasets.FashionMNIST(
        root="data",
        train=True,
        download=True,
        transform=ToTensor(),
    )

# Download test data from open datasets.
test_data = datasets.FashionMNIST(
    root="data",
    train=False,
    download=True,
    transform=ToTensor(),
    )

# Define model
class NeuralNetwork(nn.Module):
    def __init__(self):
        super(NeuralNetwork, self).__init__()
        self.flatten = nn.Flatten()
        self.linear_relu_stack = nn.Sequential(
            nn.Linear(28*28, 512),
            nn.ReLU(),
            nn.Linear(512, 512),
            nn.ReLU(),
            nn.Linear(512, 10)
        )

    def forward(self, x):
        x = self.flatten(x)
        logits = self.linear_relu_stack(x)
        return logits

def train(hyperparams, verbose=True):
    without_epochs = copy.copy(hyperparams)
    del without_epochs["epochs"]
    if hyperparams["epochs"] == 0:
        r = epoch(None, without_epochs, verbose)
        r.join()
        return r
    one_less = copy.copy(hyperparams)
    one_less["epochs"] = one_less["epochs"] -1
    #print(f"Epoch {hyperparams['epochs']}\n-------------------------------")
    return epoch(train(one_less, verbose).join(), without_epochs).join()
    
    

@dj.Task.make(mode="process", verbose=dj.Ignore, version = 0.2)
def epoch(run, hyperparams, verbose=True):
    # Get cpu or gpu device for training.
    # dj.reserve_resources(gpus=1)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    if verbose:
        print(f"Using {device} device")

    

    if run is None:
        torch.manual_seed(hyperparams["seed"])
            #random.seed(seed)
        np.random.seed(hyperparams["seed"])
        model = NeuralNetwork().to(device)
        print(model)
        
    else:
        model = NeuralNetwork().to(device)
        model.load_state_dict(torch.load(run.open("model.pth", "rb")))
        

        batch_size = hyperparams["batch_size"]
        seed = hyperparams["seed"]

        # Create data loaders.
        train_dataloader = DataLoader(training_data, batch_size=batch_size, generator=torch.Generator().manual_seed(seed))
        test_dataloader = DataLoader(test_data, batch_size=batch_size, generator=torch.Generator().manual_seed(seed))
        if verbose:
            for X, y in test_dataloader:
                print("Shape of X [N, C, H, W]: ", X.shape)
                print("Shape of y: ", y.shape, y.dtype)
                break

        
        
        loss_fn = nn.CrossEntropyLoss()
        optimizer = torch.optim.SGD(model.parameters(), lr=hyperparams["lr"])

        with run.open("rng.pth", "rb") as f:
            torch.set_rng_state(torch.load(f))

        
        size = len(train_dataloader.dataset)
        model.train()
        for batch, (X, y) in enumerate(train_dataloader):
            X, y = X.to(device), y.to(device)

            # Compute prediction error
            pred = model(X)
            loss = loss_fn(pred, y)

            # Backpropagation
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

            if batch % 100 == 0:
                loss, current = loss.item(), batch * len(X)
                if verbose:
                    print(f"loss: {loss:>7f}  [{current:>5d}/{size:>5d}]")
        size = len(test_dataloader.dataset)
        num_batches = len(test_dataloader)
        model.eval()
        test_loss, correct = 0, 0
        with torch.no_grad():
            for X, y in test_dataloader:
                X, y = X.to(device), y.to(device)
                pred = model(X)
                test_loss += loss_fn(pred, y).item()
                correct += (pred.argmax(1) == y).type(torch.float).sum().item()
        test_loss /= num_batches
        correct /= size
        print(f"Test Error: \n Accuracy: {(100*correct):>0.1f}%, Avg loss: {test_loss:>8f} \n")


    with dj.open("rng.pth", "wb+") as f:
        torch.save(torch.get_rng_state(), f)
    print("Saved Generator State")

    with dj.open("model.pth", "wb+") as f:
        torch.save(model.state_dict(), f)
    print("Saved PyTorch Model State to model.pth")

@dj.Task.make(verbose=dj.Ignore, version=3)
def get_test_predictions(run, verbose=True):
    #run.join() not possible yet
    model = NeuralNetwork()
    model.load_state_dict(torch.load(run.open("model.pth", "rb")))

    print(f"generating test predictions for run {run.run_id} with hyperparams {run.kwargs['hyperparams']}")
    model.eval()
    l_predicted = []
    for i in range(len(test_data)):
        x = test_data[i][0]
        with torch.no_grad():
            pred = model(x)
            l_predicted.append(pred[0].argmax(0))
    
    return l_predicted


def accuracy(run):
    l_predicted = get_test_predictions(run).get()
    l_correct = [dp[1] for dp in test_data]
    return accuracy_score(l_correct, l_predicted)

def f1(run):
    l_predicted = get_test_predictions(run).get()
    l_correct = [dp[1] for dp in test_data]
    return f1_score(l_correct, l_predicted, average="macro")

@dj.Task.make(mode="process", version=0.1)
def epochs_experiment(default_hyperparams, start_epoch, end_epoch, step, seeds, metrics):
    print("in epochs experiment")
    

    metrics = [eval(m) for m in metrics]
    grid = dj.Frame()
    grid["hyperparams"] = default_hyperparams
    grid["hyperparams"]["epochs"] = dj.Vary(range(start_epoch, end_epoch+1, step))
    grid["hyperparams"]["seed"] = dj.Vary(range(seeds))
    #print("meme", list(grid))
    runs = grid.map(train)
    #runs= runs.get()
    metrics_grid = {metric.__name__: runs.map(metric) for metric in metrics}
    raw_data = dj.Frame.make({**metrics_grid, "epochs": grid["hyperparams"]["epochs"]})
    #print(list(raw_data))
    data= dj.Frame(raw_data).group_by("epochs")
    #print(list(data))
    mean_grid = {metric.__name__ + " (mean)": data[metric.__name__].map(np.mean) for metric in metrics}
    std_grid = {metric.__name__ + " (std)": data[metric.__name__].map(np.std) for metric in metrics}
    data = dj.Frame.make({**mean_grid, **std_grid, "epochs": data["epochs"]})
    #print(list(data))
    df = pd.DataFrame(data)
    #print(df)
    raw_df = pd.DataFrame(raw_data)

    fig, axs = plt.subplots(len(metrics))
    if len(metrics) == 1:
        axs = [axs]
    fig.suptitle('Various Metrics with Increasing Epochs')
    for ax, metric in zip(axs, metrics):
        #ax.set_title(metric.__name__)
        ax.set_ylabel(metric.__name__)
        df[f"{metric.__name__} (mean)"].plot.bar(x=df["epochs"], yerr=df[f"{metric.__name__} (std)"], ax=ax, capsize=4, rot=0)
    with dj.open("figure.png", "wb+") as f:
        plt.savefig(f,format="png")
    with dj.open("latex.txt", "w+") as f:
        df.to_latex(f)
    
    html_string = '''
    <html>
    <head><title>Epochs Experiment Report</title></head>
    <link rel="stylesheet" type="text/css" href="df_style.css"/>
    <body>
        <img src="figure.png" alt="Figure">
        <h3>Results</3>
        {df}
        <h3>Raw</3>
        {raw_df}
    </body>
    </html>.
    '''

    # OUTPUT AN HTML FILE
    with dj.open('myhtml.html', 'w') as f:
        f.write(html_string.format(df=df.to_html(), raw_df=raw_df.to_html()))



if __name__ == "__main__":
    #dj.sync_backups()
    r1 = epochs_experiment(defaults, 0, 1, 1, seeds=1, metrics = ["accuracy", "f1"])
    r2 = epochs_experiment(defaults, 0, 1, 1, seeds=1, metrics = ["accuracy", "f1"])
    r1.join()
    r2.join()
    #dj.backup()
    print(r1.run_id, r2.run_id)
    

