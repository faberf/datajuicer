import unittest
import datajuicer as dj
import datajuicer.errors as er

class TestRun(unittest.TestCase):

    def test_run_ouput(self):

        frame1 = dj.Frame().vary("key1", [1,2,3])

        frame1 = frame1.vary("key2", [0,10])

        frame2 = dj.run(lambda a, b: a+b, frame1.select("key1"), frame1.select( "key2"))

        self.assertEqual(frame2, dj.Frame([1, 11, 2, 12, 3, 13]))
    
    def test_run_range_error(self):
        frame1 = dj.Frame([1,2,3,4])

        frame2 = dj.Frame([1,2])

        self.assertRaises(er.RangeError, dj.run, lambda a, b: a+b, frame1, frame2)

    def test_run_type_error(self):
        self.assertRaises(TypeError, dj.run, "not a function")
    
    def test_run_no_frames_input(self):
        self.assertRaises(er.NoFramesError, dj.run, lambda x: None, 5)
    
    def test_run_ids_unique(self):
        out = dj.run(lambda x, y: x, dj.RunID, dj.Frame(range(5)))
        self.assertNotEqual(out[0], out[1])
    
    def test_run_ids_unique_again(self):
        out = dj.run(lambda x, y: x, dj.RunID, dj.Frame(range(1)))
        out2 = dj.run(lambda x, y: x, dj.RunID, dj.Frame(range(1)))
        self.assertNotEqual(out[0], out2[0])