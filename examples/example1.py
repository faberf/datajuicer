import datajuicer as dj
import numpy as np

@dj.tasks.add(verbose = dj.Ignore)
def increment(n, verbose=True):
    if verbose:
        print(f"Incrementing {n}")
    return n + 1

@dj.tasks.add()
def random_vector(shape):
   vec = np.random.rand(*shape)
   with dj.open("vec.npy", "wb+") as f:
      np.save(f, vec)

@dj.tasks.add()
def factorial(n):
    print(f"Calculating factorial of {n}")
    if n == 0:
        return 1
    return dj.tasks.factorial(n-1).get()*n

@dj.tasks.add()
def mult(a, b):
    return a * b

if __name__ == "__main__":
    dj.setup(max_workers=2)

    dj.backup()

    print(dj.tasks.increment(5, force=True, launch=dj.NewProcess(), session=dj.Attach()).get())

    dj.backup()

    print(dj.tasks.increment(5, force=True, launch=dj.NewProcess(), session=dj.NewSession(1)).get())

    with dj.tasks.random_vector((10,)).open("vec.npy", "rb") as f:
        print(np.load(f))

    print(dj.tasks.factorial(5).get())

    sweep = dj.Frame()
    sweep["n"] = dj.Vary(list(range(10)) + list(range(10)))
    results = sweep.map(dj.tasks.factorial).get()
    print(list(results))

    results = dj.tasks.factorial(dj.Vary(range(10))).get()
    print(list(results))

    results = dj.tasks.mult(results, dj.Vary(list(results))).get()
    print(list(results))

    dj.backup()

    f = dj.Frame.make(a=dj.Vary(range(5)), b = 1)
    even = f.where(f["a"].map(lambda x: x % 2 == 0))
    even["b"] = dj.Vary(even["a"].map(range))
    results = dj.tasks.mult(f["a"], dj.tasks.mult(f["a"], f["b"]).get()).get()
    print(list(results))
    

