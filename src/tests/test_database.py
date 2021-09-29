import unittest
import datajuicer as dj
import datajuicer.errors as er
import shutil
from parameterized import parameterized_class

# Get directory name
def remove_folder(dir):
    try:
        shutil.rmtree(dir)
    except OSError as e:
        print("Error: %s - %s." % (e.filename, e.strerror))

@parameterized_class([
   { "database": dj.TinyDB, "path": "./tests/test_tinydb_test_" },
   { "database": dj.SQLiteDB, "path": "./tests/test_sqlitedb_test_"  },
])
class TestDatabase(unittest.TestCase):

    def test_get_no_runs(self):
        dir = self.path + "get_no_runs"
        remove_folder(dir)
        runner = dj.Runner(lambda x,y: x+y, database=self.database(dir), n_threads=4)
        runner.get_runs(dj.Frame([1,2,3,4]), dj.Frame([10,20,30,40]))
        remove_folder(dir)

    def test_record_add(self):
        dir = self.path + "record_add"
        remove_folder(dir)

        runner = dj.Runner(lambda x,y: x+y, database=self.database(dir), n_threads=4)

        runner.run(dj.Frame([1,2,3,4]), dj.Frame([10,20,30,40]))

        record = self.database(dir).get_raw()

        self.assertEqual(set([(r["arg_x"], r["arg_y"]) for r in record]), set([(1, 10), (2, 20), (3, 30), (4, 40)]))

        remove_folder(dir)
    
    def test_get_runs_add(self):
        dir = self.path + "get_runs_add"
        remove_folder(dir)

        runner = dj.Runner(lambda x,y,z: z, database=self.database(dir), n_threads=4)

        f1 = dj.Frame([1,2,3,4])

        f2 = dj.Frame([10,20,30,40])

        runner.run(f1, f2, dj.RunID)

        runner.run(f1, f2, dj.RunID)

        ids = runner.run(f1, f2, dj.RunID)

        ids2 = runner.get_runs(f1, f2, dj.Ignore)

        self.assertEqual(ids, ids2)

        remove_folder(dir)
    
    def test_get_runs_func_frame(self):

        dir = self.path + "get_runs_func_frame"
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

        runner = dj.Runner(dj.select(config, "func"), database=self.database(dir))

        ids = runner.run(dj.select(config, "z"), dj.RunID)

        ids2 = runner.get_runs(dj.select(config, "z"), dj.Ignore)

        self.assertEqual(ids, ids2)

        remove_folder(dir)


    def test_nested_ignores(self):
        dir = self.path + "nested_ignores"
        remove_folder(dir)

        runner = dj.Runner(lambda y,z: z, database=self.database(dir), n_threads=4)

        frame = dj.vary(dj.Frame.new(), "a", [1,2])
        frame = dj.vary(frame, "b", [3,4])
        frame = dj.configure(frame, {"c":frame})


        ids = [runner.run(frame[i:i+1], dj.RunID)[0] for i in range(len(frame)) ]

        nested_ignores = dj.Frame([
            {"c":{"a":1, "b":dj.Ignore}},
            {"c":{"a":1, "b":4}},
            {"c":{"a":dj.Ignore, "b":3}},
            {"c":{"a":2, "b":4}},
        ])

        should = [ids[1], ids[1], ids[2], ids[3]]

        ids2 = runner.get_runs(nested_ignores, dj.Ignore)

        if [ids.index(rid) for rid in ids2] == [1,1,2,3]:
            runner.get_runs(nested_ignores, dj.Ignore)
        else:
            runner.get_runs(nested_ignores, dj.Ignore)

        self.assertEqual(should, ids2)

        remove_folder(dir)
    
    def test_get_all_runs(self):
        dir = self.path + "get_all_runs"
        remove_folder(dir)

        runner = dj.Runner(dj.Recordable(lambda y,z: z, "func1"), database=self.database(dir), n_threads=4)

        frame = dj.Frame(range(4))

        ids = runner.run(frame, dj.RunID)
        ids += runner.run(frame, dj.RunID)

        ids2 = self.database(record_directory=dir).get_all_runs(func="func1")

        self.assertEqual(set(ids), set(ids2))
        remove_folder(dir)

    def test_delete_runs(self):
        dir = "delete_runs"
        remove_folder(dir)

        runner = dj.Runner(dj.Recordable(lambda y,z: z, "func1"), database=self.database(dir), n_threads=4)

        frame = dj.Frame(range(4))

        ids = runner.run(frame, dj.RunID)
        ids2 = runner.run(frame, dj.RunID)

        database = self.database(dir)

        database.delete_runs(ids)

        ids3 = database.get_all_runs(func="func1")

        self.assertEqual(set(ids3), set(ids2))
        remove_folder(dir)