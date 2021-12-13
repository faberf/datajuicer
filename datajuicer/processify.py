import os
import sys
import traceback
from functools import wraps, partial
from multiprocessing import Process, Queue, get_context


def processify(func):
    '''Decorator to run a function as a process.
    Be sure that every argument and the return value
    is *pickable*.
    The created process is joined, so the code does not
    run in parallel.
    '''

    

    # register original function with different name
    # in sys.modules so it is pickable
    # process_func.__name__ = func.__name__ + 'processify_func'
    # setattr(sys.modules[__name__], process_func.__name__, process_func)



    return wraps(func)(partial(wrapper, _func=func))

def process_func(q, *args,_func=None, **kwargs):
    try:
        ret = _func(*args, **kwargs)
    except Exception:
        ex_type, ex_value, tb = sys.exc_info()
        error = ex_type, ex_value, ''.join(traceback.format_tb(tb))
        ret = None
    else:
        error = None

    q.put((ret, error))

def wrapper(*args,_func=None, **kwargs):
    ctx = get_context('spawn')
    target = wraps(_func)(partial(process_func, _func=_func))
    q = ctx.Queue()
    p = ctx.Process(target=target, args=[q] + list(args), kwargs=kwargs)
    p.start()
    ret, error = q.get()
    p.join()

    if error:
        ex_type, ex_value, tb_str = error
        message = '%s (in subprocess)\n%s' % (ex_value.args, tb_str)
        raise ex_type(message)

    return ret