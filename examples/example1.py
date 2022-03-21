import datajuicer as dj
import pandas as pd
import time
import time

tasks = dj.TaskList()

@tasks(version=0.4)
def mytask(a, b):
    dj.reserve_resources(ram_gb=10)
    print("hi")
    time.sleep(2)
    
    return a ** b

#mytask = dj.Task.make(name="mytask", version=0.99)(_mytask)

@tasks(version=0.21)
def myhighertask(start_a, end_a, start_b, end_b):
    print(f"cheese{time.time()}")
    f = dj.Frame()
    f["a"] = dj.Vary(range(start_a, end_a))
    f["b"] = dj.Vary(range(start_b, end_b))
    f["results"] = f.map(tasks.mytask.with_launcher(dj.NewThread())).get()
    f.group_by("b")
    pd.DataFrame(f).to_csv(dj.open("output.csv", "w+"), index=False)

@tasks()
def bla(a=1, A=2):
    print(f"a is {a}, A is {A}")


if __name__ == "__main__":
    
    #dj.sync_backups()
    dj.setup(max_workers=1, clean=True)
    task = tasks.myhighertask
    task.force()
    task.in_new_session(2, ram_gb=20)
    launcher = dj.NewProcess()
    launcher = dj.Command("jbsub -interactive -require a100 -cores 16+1 -mem 32g -q x86_1h COMMAND")
    task.with_launcher(launcher)
    run = task(0, 10, 0, 4)
    run.join()
    with run.open("output.csv", "r") as f:
        df = pd.read_csv(f)
    print(df)
    # dj.setup(1)
    # tasks.bla.force()(4,5).join()
    #dj.backup()

