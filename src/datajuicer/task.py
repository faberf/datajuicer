import datajuicer as dj
import copy
import os

class Task:
    name = "task"
    parents = ()
    
    @classmethod
    def setup(cls, data, directory, force, incognito, n_threads):
        return data
    
    @staticmethod
    def preprocess(data):
        pass

    @staticmethod
    def postprocess(data, output):
        pass 

    @staticmethod
    def compute(datapoint, run_id):
        pass
    
    @staticmethod
    def check(datapoint, run_id):
        return True

    @staticmethod
    def load(datapoint, run_id):
        pass

    @staticmethod
    def get_dependencies(self, datapoint):
        return datapoint.keys()
    
    @classmethod
    def configure(cls, data, directory=".", force=False, incognito=False, n_threads=1):
        output, rids = cls.run(data, directory, force, incognito, n_threads, True)
        return dj.configure(data, {cls.name +"_run_id": rids, cls.name + "_output": output})


    @classmethod
    def run(cls, data, directory=".", force=False, incognito=False, n_threads=1, return_run_ids=False):
        where_already_loaded = dj.Where(data, [cls.name+"_run_id" in dp for dp in data])
        data = where_already_loaded.false

        for parent in cls.parents:
            data = parent.configure(data, directory, force, incognito, n_threads)

        data = cls.setup(data, directory, force, incognito, n_threads)

        @dj.recordable(cls.name)
        def run_and_return_id(datapoint, rid):
            cls.compute(datapoint = datapoint, run_id=rid)
            return rid

        dependencies = dj.Frame([{key:datapoint[key] for key in cls.get_dependencies(copy.copy(datapoint)) + [parent.name +"_run_id" for parent in cls.parents]} for datapoint in data])

        getter = dj.Getter(run_and_return_id, directory)
        run_ids = getter.get_runs(dependencies, dj.Ignore)

        def needs_rerun(datapoint, run_id):
            if force:
                return True
            if run_id is None:
                return True
            return not cls.check(datapoint, run_id)

        where_already_run = dj.Where(dj.run(needs_rerun, data, run_ids))

        if incognito:
            record_directory = None
        else:
            record_directory = directory
        runner = dj.Runner(run_and_return_id, n_threads=n_threads,record_directory=record_directory)
        
        new_data = where_already_run.false(data)
        cls.preprocess(new_data)
        u = dj.Unique(where_already_run.false(dependencies))
        new_data_canonical = dj.Where(new_data, u.is_canonical).true
        new_run_ids_canonical = runner.run(new_data_canonical, dj.RunID)
        new_run_ids = u.expand(new_run_ids_canonical)

        all_rids = where_already_run.join(where_already_run.true(run_ids), new_run_ids)
        
        output = dj.run(cls.load, data, all_rids)

        cls.postprocess(new_data, output)

        output = where_already_loaded.join(dj.select(where_already_loaded.true, cls.name + "_output"), output)
        all_rids = where_already_loaded.join(dj.select(where_already_loaded.true, cls.name + "_run_id"), all_rids)

        if return_run_ids:
            return output, all_rids
        else:
            return output