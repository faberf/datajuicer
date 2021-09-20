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

        runner = dj.Runner(lambda x,y: x+y, "add", record_directory=dir, n_threads=4)

        runner.run(dj.Frame([1,2,3,4]), dj.Frame([10,20,30,40]))

        db = tinydb.TinyDB(os.path.join(dir, "runs.json"))

        record = db.all()

        self.assertEquals([(r["arg_x"], r["arg_y"]) for r in record], [(1, 10), (2, 20), (3, 30), (4, 40)])

        remove_folder(dir)
    
    def test_get_runs_add(self):
        dir = "./tests/test_recording_test_get_runs_add"
        remove_folder(dir)

        runner = dj.Runner(lambda x,y,z: z, "add", record_directory=dir, n_threads=4)

        f1 = dj.Frame([1,2,3,4])

        f2 = dj.Frame([10,20,30,40])

        runner.run(f1, f2, dj.RunID)

        runner.run(f1, f2, dj.RunID)

        ids = runner.run(f1, f2, dj.RunID)

        getter = dj.Getter(lambda x,y,z:z, "add", record_directory=dir)

        ids2 = getter.get_runs(f1, f2, dj.Ignore)

        self.assertEquals(ids, ids2)

        remove_folder(dir)