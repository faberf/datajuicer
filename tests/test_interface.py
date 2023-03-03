import threading
from datajuicer.core.redirect import Redirect
from datajuicer.cache.equality_query import make_query
import unittest
import shutil, os, time 
import datajuicer as dj

def myfunc(a, b, start):
    print("in myfunc", a, b, "%.2f" % (time.time() - start))
    time.sleep(1.0)
    return b


class TestInterface(unittest.TestCase):
    
    def setUp(self) -> None:
        self.dirname = f"tests/tmp/test_interface_{self._testMethodName}"
        self.tearDown()
    
    def test_all(self):
        dj.set_directory(self.dirname)
        start = time.time()

        with dj.Session(3):
            t1 = dj.Task(myfunc, 1, 2, dj.Any(start))
            t2 = dj.Task(myfunc, 12, 22, dj.Any(start))
            t3 = dj.Task(myfunc, 13, 23, dj.Any(start))

            t1.run(launcher=dj.Direct())
            t2.run(launcher=dj.NewThread())
            t3.run(launcher=dj.NewThread())
        
        self.assertEqual(t1.get(), 2)
        self.assertEqual(t2.get(), 22)
        self.assertEqual(t3.get(), 23)
        
    def tearDown(self) -> None:
        if os.path.isdir(self.dirname):
            shutil.rmtree(self.dirname)

if __name__ == '__main__':
    unittest.main()