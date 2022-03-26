# Datajuicer Quickstart

Datajuicer is a library that takes care of parallelism, caching and synchronization for dynamic and complex computations.

## Basic Example

At its core, this is how to use datajuicer.

1. Import the library `import datajuicer as dj`
2. Define your own tasks and decorate them with `@dj.tasks.add()`
3. Call `dj.setup()`
4. Run your tasks:
   1. Access your task `task = dj.tasks.<TASK_NAME>`
   2. Create a run `run = task(<ARGS>)`
   3. Obtain the result `result = run.get()`

```
import datajuicer as dj

@dj.tasks.add()
def increment(n):
    print(f"Incrementing {n}")
    return n + 1

dj.setup()
print(dj.tasks.increment(5).get())
```

## Caching

If you rerun the above program, there is no console out "Incrementing 5". This is because, by default, datajuicer caches everything. If you would like to recompute you have a couple of options:

* Setting the `force` keyword to `True`
   
   Replacing `dj.tasks.increment(5)` with `dj.tasks.increment(5, force=True)` will recompute the task each time and cache the result.

* Setting the `incognito` keyword to `True`
   
   Using `dj.tasks.increment(5, incognito=True)` will still use a cached result if available but if it has to compute it will not save its result.

* Setting both `incognito=True` and `force=True`
    
    This will ignore the cache completely.

* When decorating a task in `dj.tasks.add` you can specify a `version` keyword as follows:
   
   ```
   @dj.tasks.add(version=2.0)
   def increment(n):
      print(f"Incrementing {n} (version 2.0)")
      return n + 1
   ```
   Now all runs of `increment` with an earlier version and all parents of such tasks will be rerun when called.

* A run can be manually deleted after its not useful anymore: 
   
   ```
   run = dj.tasks.increment(5)
   result = run.get()
   run.delete()
   ```

   The next time it will have to be recomputed.

If you have some arguments of a task that do not affect the output at all you can tell datajuicer to ignore them:

```
@dj.tasks.add(verbose = dj.Ignore)
def increment(n, verbose=True):
   if verbose:
      print(f"Incrementing {n}")
   return n + 1
```

Or, equivalently:

```
@dj.tasks.add(n = dj.Keep)
def increment(n, verbose=True):
   if verbose:
      print(f"Incrementing {n}")
   return n + 1
```


Using `dj.Depend`, you can also do more fancy stuff with dependencies. (Outside the scope of this tutorial.)

Besides the functional input and output of a function, you can also cache side effects using datajuicer. Just replace `open` function from python with `dj.open` when you are inside a task. Later when the run has executed, you can retrieve these files using `run.open`. It is recommended to handle all data that does not like being pickled in this way. Here is an example:

```
@dj.tasks.add()
def random_vector(shape):
   vec = np.random.rand(*shape)
   with dj.open("vec.npy", "wb+") as f:
      np.save(f, vec)

dj.setup()
with dj.tasks.random_vector((10,)).open("vec.npy", "rb") as f:
   print(np.load(f))

```



## Add Some Recursion

Tasks can be launched from within tasks:

```
import datajuicer as dj

@dj.tasks.add()
def factorial(n):
    print(f"Calculating factorial of {n}")
    if n == 0:
        return 1
    return dj.tasks.factorial(n-1).get()*n

if __name
dj.setup()
print(dj.tasks.factorial(5).get())
```

## Define A Sweep

To define sweeps you should use datajuicer frames. You can think of a frame as a list where each element contains the arguments to a specific run in the sweep.

```
sweep = dj.Frame()
sweep["n"] = dj.Vary(range(10))
results = sweep.map(dj.tasks.factorial).get()
print(list(results))
```

Lets break this down:
1. Create the frame of inputs to factorial.
   1. `sweep = dj.Frame()` 
   2. `sweep["n"] = dj.Vary(range(10))`
   
      `sweep` is now a frame containing `[{"n":0}, {"n":1}, ...]`
      This can be verified by calling `list(sweep)`

2. Create a frame of runs.
   1. `run_frame = sweep.map(dj.tasks.factorial)`
   
      Calling `map` on a frame with a function returns another frame containing the output of that function on each element of the frame.
      Since `dj.tasks.factorial` returns a Run object, we obtain a frame of Run objects.

3. `results = run_frame.get()` collects the results using the `get` method which returns a frame with the result of each Run. 
4. Convert to list and print.

We can also create a sweep implicitely:

```
results = dj.tasks.factorial(dj.Vary(range(10))).get()
print(list(results))
```

If you look at the terminal output you will see that we never recompute any factorial that has already been calculated.

## Parallelism

In datajuicer, parallelism is really easy! Just specify your maximum number of workers in the `setup` method:

```
dj.setup(max_workers=10)
```

If there is some system resource that is finite and you want datajuicer to handle exclusion, specify them here as well:

```
dj.setup(max_workers=10, ram=128, bandwidth=10) #ram and bandwidth are just symbols with no meaning to datajuicer
```

Then later in a task, reserve the resources as follows (workers do not need to be reserved, they are handled automatically):

```
dj.reserve_resources(ram=1.5)
```

## Threads, Processes, Jobs

By default, each run launches in its own thread. There are however other options that you can specify when you create a run with the `launch` keyword.

* `launch=dj.Direct()` launches the run directly in the same thread.
* `launch=dj.NewThread()` launches the run in a new thread (this is the default).
* `launch=dj.NewProcess()` launches the run in a new process.
* `launch=dj.Command(<TEMPLATE>)` lets the user define a new launch mode. A template is a string that contains the substring `COMMAND`.
   For example: `launch=dj.Command("bsub -interactive -require a100 -cores 16+1 -mem 32g -q x86_1h COMMAND")`

Often times, especially with batch submissions commands like `bsub`, you do not want the runs to share the same system resources and count towards the `max_workers`. For these cases the `session` keyword comes in handy:

* `session=dj.Attach()` This is the default.
* `session=dj.NewSession(max_workers, **resources)` This lets the run launch in its own session. `dj.NewSession` takes the same keyswords as `dj.setup`.


## Some More Fun with Frames

```
f = dj.Frame()
f["b"] = 1
f["a"] = dj.Vary(range(5))
f.where([a % 2 == 0 for a in f["a"]])["b"] = dj.Vary([1,2])
```

```
f = dj.Frame.make(a=dj.Vary(range(5)), b = 1)
even = f.where(f["a"].map(lambda x: x % 2 == 0))
even["b"] = dj.Vary(even["a"].map(range))
results = dj.tasks.mult(f["a"], dj.tasks.mult(f["a"], f["b"]).get()).get()
```