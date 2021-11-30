import datajuicer as dj
import pandas as pd
import time

dj.setup(max_n_threads=5, ram_gb = 30)
dj.sync_backups()

@dj.Task.make(version=0.7)
def mytask(a, b):
    print("hi")
    dj.reserve_resources(ram_gb=10)
    return a ** b

@dj.Task.make(version=0.5)
def myhighertask(start_a, end_a, start_b, end_b):
    print(f"cheese{time.time()}")
    f = dj.Frame()
    f["a"] = dj.Vary(range(start_a, end_a))
    f["b"] = dj.Vary(range(start_b, end_b))
    f["results"] = f.map(mytask).get()
    f.group_by("b")
    pd.DataFrame(f).to_csv(dj.open("output.csv", "w+"), index=False)



run = myhighertask(0, 10, 0, 4)
run.join()
with run.open("output.csv", "r") as f:
    df = pd.read_csv(f)
print(df)
dj.backup()
