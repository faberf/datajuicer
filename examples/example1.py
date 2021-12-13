import datajuicer as dj
import pandas as pd
import time
import functools
import time

if __name__ == "__main__":
    dj.setup(max_workers=3)
    dj.free_resources(ram_gb = 10)

@dj.Task.make(version=0.26, mode="process")
def mytask(a, b):
    #dj.reserve_resources(ram_gb=10)
    print("hi")
    time.sleep(2)
    
    return a ** b

#mytask = dj.Task.make(name="mytask", version=0.99)(_mytask)

@dj.Task.make(version=0.0, mode="process")
def myhighertask(start_a, end_a, start_b, end_b):
    print(f"cheese{time.time()}")
    f = dj.Frame()
    f["a"] = dj.Vary(range(start_a, end_a))
    f["b"] = dj.Vary(range(start_b, end_b))
    f["results"] = f.map(mytask).get()
    f.group_by("b")
    pd.DataFrame(f).to_csv(dj.open("output.csv", "w+"), index=False)




if __name__ == "__main__":
    dj.free_resources(ram_gb = 30)
    #dj.sync_backups()
    run = myhighertask(0, 10, 0, 4)
    run.join()
    with run.open("output.csv", "r") as f:
        df = pd.read_csv(f)
    print(df)
    #dj.backup()

