import argparse
import datajuicer
from datajuicer._global import GLOBAL
import threading
import sys
import traceback
import dill

class Namespace:
    pass



if __name__ == "__main__":
    try:
        ap = argparse.ArgumentParser()
        ap.add_argument("-path", type=str)
        path = ap.parse_args().path
        with open(path, "rb") as f:
            kwargs, task_info, force, incognito, parent_task_name, parent_task_version, parent_run_id = dill.load(f)
        
        task = datajuicer.task.make(*task_info[0:-2], **task_info[-2])(task_info[-1])

        curthread = threading.current_thread()
        assert(type(curthread) != datajuicer.task.Run)

        datajuicer.logging.enable_proxy()
        GLOBAL.resource_lock = task.resource_lock
        GLOBAL.cache = task.cache
        run = datajuicer.Run(task, kwargs, force, incognito, False)
        pseudo_parent = Namespace()
        pseudo_parent.task = Namespace()
        if not parent_task_name is None:
            pseudo_parent.task.name = parent_task_name
            pseudo_parent.task.version = parent_task_version
            pseudo_parent.run_id = parent_run_id
            run.start(pseudo_parent)
        else:
            run.start()

        ret = run.get()
    except Exception:
        ex_type, ex_value, tb = sys.exc_info()
        error = ex_type, ex_value, ''.join(traceback.format_tb(tb))
        ret = None
    else:
        error = None
    
    with open(path+ "out", "wb+") as f:
        dill.dump((ret, error), f)
