from datajuicer.cache.tinydbcache import TinyDBCache
from datajuicer.cache.equality_query import make_query
import unittest
import shutil, os
from parameterized import parameterized_class

def myfunc():
    pass

def myotherfunc():
    pass

class globaltype1:
    pass

class globaltype2:
    pass

@parameterized_class([
   { "cachetype": TinyDBCache},
])
class TestCache(unittest.TestCase):
    
    def setUp(self) -> None:
        self.dirname = f"tests/tmp/{self.cachetype.__name__}_{self._testMethodName}"
        self.tearDown()
        self.cache = self.cachetype(self.dirname)
        
    def insert_and_search(self, *data):
        for i,dp in enumerate(data):
            dp["id"] = f"testid{i}"
            self.cache.insert(dp)
        for dp in data:
            res = self.cache.search(make_query(dp))
            self.assertTrue(len(res) == 1)

    def test_insert_string(self):
        self.insert_and_search({"data": "hi"},{"data": "hello"})

    def test_insert_int(self):
        self.insert_and_search({"data": 42},{"data": 69})
    
    def test_insert_func(self):
        self.insert_and_search({"data": myfunc},{"data": myotherfunc})
    
    def test_insert_local_func(self):
        def f1():
            pass
        def f2():
            pass
        self.insert_and_search({"data": f1},{"data": f2})
    
    def test_insert_dict(self):
        self.insert_and_search({"data": {"a":1}},{"data": {"a":1, "b":1}})
    
    def test_insert_local_type(self):
        class t1:
            pass
        class t2:
            pass
        self.insert_and_search({"data":t1}, {"data":t2})
        
    def test_insert_global_type(self):
        self.insert_and_search({"data":globaltype1}, {"data":globaltype2})
    
    def test_insert_interactions(self):
        def f1():
            pass
        class t1:
            pass
        self.insert_and_search(
            {"data": "hi"},
            {"data": 42},
            {"data": myfunc},
            {"data": f1},
            {"data": {"a":1}},
            {"data":t1},
            {"data":globaltype1}
        )
    
    def tearDown(self) -> None:
        if os.path.isdir(self.dirname):
            shutil.rmtree(self.dirname)

if __name__ == '__main__':
    unittest.main()