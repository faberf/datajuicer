import datajuicer as dj
import copy
import os.path
import json, pickle

class Cache:
    force_load = False
    database = dj.BaseDatabase()

    def __init__(self):
        pass

    def load(self, datapoint, run_id):
        pass

    def check(self, datapoint, run_id):
        pass
    
    def save(self, output, datapoint, run_id):
        pass
    
    def __call__(self, task):
        task.cache = self
        self.task = task
        return task

class NoCache(Cache):
    database = dj.BaseDatabase()

    def check(self, datapoint, run_id):
        return False

class FileCache(Cache):
    def __init__(self, files, directory="."):
        self.force_load = True
        self.files = files
        self.directory = directory
        
        self.database = dj.FastSQLiteDB(directory)

    def load(self, _, run_id):
        serializers = {"pickle":pickle, "json":json}
        modes = {"pickle":"rb", "json":"r"}
        out = {}
        for f in self.files:
            file_name, file_end = f.split(".")
            path = os.path.join(self.directory, f"{self.task.name}_{run_id}_{f}")
            with open(path, modes[file_end]) as file:
                out[file_name] = serializers[file_end].load(file)
        
        return out
    
    def check(self, _, run_id):
        for f in self.files:
            path = os.path.join(self.directory, f"{self.task.name}_{run_id}_{f}")
            if not os.path.isfile(path):
                return False
        return True
    
    def clean_up(self):
        files = [f for f in os.listdir(self.directory) if os.path.isfile(os.path.join(self.directory, f))]

        all_ids = self.database.get_all_runs(self.task.name)

        for f in files:
            offset = len(self.task.name)+1
            extracted_rid = f[offset:offset + dj.utils.ID_LEN]
            has_correct_ending = any([f.endswith(file_ending) for file_ending in self.files])
            if not extracted_rid in all_ids and has_correct_ending and f.startswith(self.task.name):
                os.remove(os.path.join(self.directory,f))
        
        self.database.delete_runs(self.task.name,[rid for rid in all_ids if not self.check(None, rid)])


class AutoCache(Cache):
    def __init__(self, directory="."):
        self.force_load = False
        self.directory = directory
        
        self.database = dj.FastSQLiteDB(directory)
    
    def make_path(self, run_id):
        return os.path.join(self.directory, f"{self.task.name}_{run_id}.autocache")

    def load(self, _, run_id):
        with open(self.make_path(run_id), "rb") as f:
            return pickle.load(f)
    
    def check(self, _, run_id):
        return os.path.isfile(self.make_path(run_id))

    def save(self, output, _, run_id):
        with open(self.make_path(run_id), "wb+") as f:
            pickle.dump(output, f)
    
    def clean_up(self):
        files = [f for f in os.listdir(self.directory) if os.path.isfile(os.path.join(self.directory, f))]

        all_ids = self.database.get_all_runs(self.task.name)

        for f in files:
            offset = len(self.task.name)+1
            extracted_rid = f[offset:offset + dj.utils.ID_LEN]
            if not extracted_rid in all_ids and (f.endswith(".autocache")) and f.startswith(self.task.name):
                os.remove(os.path.join(self.directory,f))
        
        def bad_id(run_id):
            return not os.path.isfile(os.path.join(self.directory, f"{self.task.name}_{run_id}.autocache"))
        
        self.database.delete_runs(self.task.name,[rid for rid in all_ids if not self.check(None, rid)])


@AutoCache()
class Task:
    name = "task"
    parents = ()

    @staticmethod
    def compute(datapoint, run_id):
        pass

    @staticmethod
    def get_dependencies(datapoint):
        return datapoint.keys()
    
    @classmethod
    def run(cls, data, force=False, incognito=False, n_threads=1):
        output, _ = cls._run(data, force, incognito, n_threads)
        return output

    @classmethod
    def configure(cls, data, force=False, incognito=False, n_threads=1):
        for parent in cls.parents:
            parent_output, parent_run_ids = parent._run(data, force, incognito, n_threads)
            data = data.configure({parent.name+"_run_id": parent_run_ids, parent.name + "_output":parent_output})
        output, run_ids = cls._run(data, force, incognito, n_threads)
        return data.configure({cls.name+"_run_id": run_ids, cls.name + "_output":output})

    @classmethod
    def _run(cls, data, force, incognito, n_threads):
        alldata = data
        where_already_loaded = dj.Where([cls.name+"_run_id" in dp for dp in alldata])
        data = where_already_loaded.false(alldata)

        for parent in cls.parents:
            data = parent.configure(data, force, incognito, n_threads)

        @dj.recordable(cls.name)
        def run_and_save(datapoint, rid):
            returned = cls.compute(datapoint = datapoint, run_id=rid)
            cls.cache.save(returned, datapoint, rid)
            return {"run_id":rid, "returned":returned}
        

        dependencies = dj.Frame([{key:datapoint[key] for key in cls.get_dependencies(copy.copy(datapoint))} for datapoint in data])

        runner = dj.Runner(run_and_save, n_threads, cls.cache.database)
        run_ids = runner.get_runs(dependencies, dj.Ignore)

        def needs_rerun(datapoint, run_id):
            if force:
                return True
            if run_id is None:
                return True
            return not cls.cache.check(datapoint, run_id)

        where_needs_rerun = dj.Where(dj.run(needs_rerun, data, run_ids))

        if incognito:
            runner = dj.Runner(run_and_save, n_threads)
        
        new_data = where_needs_rerun.true(data)
        #new_data = cls.preprocess(new_data)
        unique_new_data = dj.Unique(where_needs_rerun.true(dependencies))
        new_runs_canonical = runner.run(unique_new_data.where_canonical.true(new_data), dj.RunID)
        new_run_ids = unique_new_data.expand(new_runs_canonical.select( "run_id"))
        new_run_returns = unique_new_data.expand(new_runs_canonical.select( "returned"))

        all_rids = where_needs_rerun.join(new_run_ids, where_needs_rerun.false(run_ids))
        
        if cls.cache.force_load:
            output = dj.run(cls.cache.load, data, all_rids)
        else:
            output = where_needs_rerun.join(new_run_returns, dj.run(cls.cache.load, where_needs_rerun.false(data), where_needs_rerun.false(run_ids)))

        output = where_already_loaded.join(where_already_loaded.true(alldata).select( cls.name + "_output"), output)
        all_rids = where_already_loaded.join(where_already_loaded.true(alldata).select(cls.name + "_run_id"), all_rids)

        return output, all_rids
    
    @classmethod
    def reduce(cls, frame):
        task_outputs = frame.select(cls.name + "_" + "output")
        task_rids = frame.select(cls.name + "_" + "run_id")
        return dj.Unique(task_rids).where_canonical.true(task_outputs)