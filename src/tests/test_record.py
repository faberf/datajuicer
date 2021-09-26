import os
import unittest
import datajuicer as dj
import datajuicer.errors as er
import tinydb
import shutil

# Get directory name
def remove_folder(dir):
    try:
        shutil.rmtree(dir)
    except OSError as e:
        print("Error: %s - %s." % (e.filename, e.strerror))

class TestRecording(unittest.TestCase):

    def test_record_add(self):
        dir = "./tests/test_recording_test_record_add"
        remove_folder(dir)

        runner = dj.Runner(lambda x,y: x+y, record_directory=dir, n_threads=4)

        runner.run(dj.Frame([1,2,3,4]), dj.Frame([10,20,30,40]))

        db = tinydb.TinyDB(os.path.join(dir, "runs.json"))

        record = db.all()

        self.assertEquals([(r["arg_x"], r["arg_y"]) for r in record], [(1, 10), (2, 20), (3, 30), (4, 40)])

        remove_folder(dir)
    
    def test_get_runs_add(self):
        dir = "./tests/test_recording_test_get_runs_add"
        remove_folder(dir)

        runner = dj.Runner(lambda x,y,z: z, record_directory=dir, n_threads=4)

        f1 = dj.Frame([1,2,3,4])

        f2 = dj.Frame([10,20,30,40])

        runner.run(f1, f2, dj.RunID)

        runner.run(f1, f2, dj.RunID)

        ids = runner.run(f1, f2, dj.RunID)

        getter = dj.Getter(lambda x,y,z:z, record_directory=dir)

        ids2 = getter.get_runs(f1, f2, dj.Ignore)

        self.assertEquals(ids, ids2)

        remove_folder(dir)
    
    def test_get_runs_func_frame(self):

        dir = "./tests/test_recording_test_get_runs_func_frame"
        remove_folder(dir)

        def func1(x, rid):
            return rid

        def func2(y, rid):
            return rid
        
        config1 = dj.configure(dj.Frame.new(), {"func":func1})
        config2 = dj.configure(dj.Frame.new(), {"func":func2})

        config1 = dj.vary(config1, "z", [11,12])
        config2 = dj.vary(config2, "z", [21,22])

        config = config1 + config2

        ids = dj.Runner(dj.select(config, "func"), record_directory=dir).run(dj.select(config, "z"), dj.RunID)

        ids2 = dj.Getter(dj.select(config, "func"), record_directory=dir).get_runs(dj.select(config, "z"), dj.Ignore)

        self.assertEquals(ids, ids2)

        remove_folder(dir)


    def test_nested_ignores(self):
        dir = "./tests/test_recording_test_nested_ignores"
        remove_folder(dir)

        runner = dj.Runner(lambda y,z: z, record_directory=dir, n_threads=4)

        frame = dj.vary(dj.Frame.new(), "a", [1,2])
        frame = dj.vary(frame, "b", [3,4])
        frame = dj.configure(frame, {"c":frame})


        ids = runner.run(frame, dj.RunID)

        getter = dj.Getter(lambda y,z:z, record_directory=dir)

        nested_ignores = dj.Frame([
            {"c":{"a":1, "b":dj.Ignore}},
            {"c":{"a":1, "b":4}},
            {"c":{"a":dj.Ignore, "b":3}},
            {"c":{"a":2, "b":4}},
        ])

        should = [ids[1], ids[1], ids[2], ids[3]]

        ids2 = getter.get_runs(nested_ignores, dj.Ignore)

        self.assertEquals(should, ids2)

        remove_folder(dir)
    
    def test_get_all_runs(self):
        dir = "./tests/test_recording_test_get_all_runs"
        remove_folder(dir)

        runner = dj.Runner(dj.Recordable(lambda y,z: z, "func1"), record_directory=dir, n_threads=4)

        frame = dj.Frame(range(4))

        ids = runner.run(frame, dj.RunID)
        ids += runner.run(frame, dj.RunID)

        ids2 = dj.get_all_runs(record_directory=dir, func="func1")

        self.assertEquals(ids, ids2)
        remove_folder(dir)

    def test_delete_runs(self):
        dir = "./tests/test_recording_test_delete_runs"
        remove_folder(dir)

        runner = dj.Runner(dj.Recordable(lambda y,z: z, "func1"), record_directory=dir, n_threads=4)

        frame = dj.Frame(range(4))

        ids = runner.run(frame, dj.RunID)
        ids2 = runner.run(frame, dj.RunID)

        dj.delete_runs(dir,ids)

        ids3 = dj.get_all_runs(record_directory=dir, func="func1")

        self.assertEquals(ids3, ids2)
        remove_folder(dir)