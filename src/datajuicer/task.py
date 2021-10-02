import datajuicer as dj
import copy

class Task:
    name = "task"
    parents = ()
    
    @staticmethod
    def preprocess(data):
        return data

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
    def configure(cls, data, database=dj.BaseDatabase(), force=False, incognito=False, n_threads=1):
        output, rids = cls.run(data, database, force, incognito, n_threads, True)
        return dj.configure(data, {cls.name +"_run_id": rids, cls.name + "_output": output})


    @classmethod
    def run(cls, data, database=dj.BaseDatabase(), force=False, incognito=False, n_threads=1, return_run_ids=False):
        where_already_loaded = dj.Where([cls.name+"_run_id" in dp for dp in data])
        data = where_already_loaded.false(data)

        for parent in cls.parents:
            data = parent.configure(data, database, force, incognito, n_threads)

        @dj.recordable(cls.name)
        def run_and_return_id(datapoint, rid):
            cls.compute(datapoint = datapoint, run_id=rid)
            return rid

        dependencies = dj.Frame([{key:datapoint[key] for key in cls.get_dependencies(copy.copy(datapoint)) + [parent.name +"_run_id" for parent in cls.parents]} for datapoint in data])

        runner = dj.Runner(run_and_return_id, n_threads, database)
        run_ids = runner.get_runs(dependencies, dj.Ignore)

        def needs_rerun(datapoint, run_id):
            if force:
                return True
            if run_id is None:
                return True
            return not cls.check(datapoint, run_id)

        where_needs_rerun = dj.Where(dj.run(needs_rerun, data, run_ids))

        if incognito:
            runner = dj.Runner(run_and_return_id, n_threads)
        
        new_data = where_needs_rerun.true(data)
        new_data = cls.preprocess(new_data)
        unique_new_data = dj.Unique(where_needs_rerun.true(dependencies))
        new_run_ids_canonical = runner.run(unique_new_data.where_canonical.true(new_data), dj.RunID)
        new_run_ids = unique_new_data.expand(new_run_ids_canonical)

        all_rids = where_needs_rerun.join(new_run_ids, where_needs_rerun.false(run_ids))
        
        output = dj.run(cls.load, data, all_rids)

        output = where_already_loaded.join(dj.select(where_already_loaded.true(data), cls.name + "_output"), output)
        all_rids = where_already_loaded.join(dj.select(where_already_loaded.true(data), cls.name + "_run_id"), all_rids)

        if return_run_ids:
            return output, all_rids
        else:
            return output