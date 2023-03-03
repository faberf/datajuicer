import threading
from datajuicer.core.redirect import Redirect
from datajuicer.cache.equality_query import make_query
import unittest
import shutil, os

from datajuicer.utils import make_dir

class TestRedirect(unittest.TestCase):
    
    def setUp(self) -> None:
        self.dirname = f"tests/tmp/test_redirect_{self._testMethodName}"
        self.tearDown()
    
    def test_print(self):
        filename = os.path.join(self.dirname,"log.txt")
        make_dir(filename)
        with Redirect(open(filename, "w+")):
            print("hello1")
        print("hello2")
        with open(filename) as f:
            s = f.read()
            self.assertEqual(s, "hello1\n")
    
    def test_print_nested(self):
        filenameouter = os.path.join(self.dirname,"logouter.txt")
        filenameinner = os.path.join(self.dirname,"loginner.txt")
        make_dir(filenameouter)
        make_dir(filenameinner)
        with Redirect(open(filenameouter, "w+")):
            print("hello1")
            with Redirect(open(filenameinner, "w+")):
                print("hello2")
        print("hello3")
        with open(filenameouter) as f:
            s = f.read()
            self.assertEqual(s, "hello1\nhello2\n")
        with open(filenameinner) as f:
            s = f.read()
            self.assertEqual(s, "hello2\n")
    
    def test_print_sequential(self):
        filename = os.path.join(self.dirname,"log1.txt")
        make_dir(filename)
        with Redirect(open(filename, "w+")):
            print("hello1")
        print("hello2")
        with open(filename) as f:
            s = f.read()
            self.assertEqual(s, "hello1\n")
        
        
        filename = os.path.join(self.dirname,"log2.txt")
        make_dir(filename)
        with Redirect(open(filename, "w+")):
            print("hello3")
        print("hello4")
        with open(filename) as f:
            s = f.read()
            self.assertEqual(s, "hello3\n")
        
        
        filename = os.path.join(self.dirname,"log1.txt")
        make_dir(filename)
        with Redirect(open(filename, "w+")):
            print("hello5")
        print("hello6")
        with open(filename) as f:
            s = f.read()
            self.assertEqual(s, "hello5\n")
    
    def test_print_multithread(self):
        class Thread(threading.Thread):
            def __init__(self, filename):
                self.filename = filename
                super().__init__()
            
            def run(self) -> None:
                make_dir(self.filename)
                f = open(self.filename, "w+")
                with Redirect(f):
                    print(f"hello1_{self.filename}")
                f.close()
                print(f"hello2_{self.filename}")
        
        for _ in range(50):
            ts = [Thread(os.path.join(self.dirname,f"log{i}.txt")) for i in range(10)]
            for t in ts:
                t.start()
            for t in ts:
                with open(t.filename) as f:
                    self.assertEqual(f.read(), f"hello1_{t.filename}\n")
            self.setUp()
            
    
    def tearDown(self) -> None:
        if os.path.isdir(self.dirname):
            shutil.rmtree(self.dirname)

if __name__ == '__main__':
    unittest.main()