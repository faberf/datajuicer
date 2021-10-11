from os import stat
import unittest
from tests import remove_folder
import datajuicer as dj
import operator

class TestTask(unittest.TestCase):
    path = "test_task_test_"

    def test_output(self):

        dir = self.path + "output"
        remove_folder(dir)

        @dj.AutoCache(directory=dir)
        class MyTask(dj.Task):
            computations = 0
            @staticmethod
            def compute(datapoint, run_id):
                MyTask.computations += 1
                return datapoint["x"] + datapoint["y"]
            
            @staticmethod
            def get_dependencies(datapoint):
                return "x", "y"
        
        mygrid = dj.Frame().vary( "x", [1,2,3])
        mygrid = mygrid.configure({"y":dj.Frame([9,8,7])})
        mygrid = mygrid.vary("y", [mygrid.select( "y"), dj.run(operator.mul, mygrid.select( "y"), 2)])

        output = (MyTask.run(mygrid), MyTask.computations)

        should = (dj.run(operator.add, mygrid.select( "x"), mygrid.select( "y")), 6)

        self.assertEqual(output, should)
        remove_folder(dir)
    
    def test_multiple_runs(self):

        dir = self.path + "multiple_runs"
        remove_folder(dir)

        @dj.AutoCache(directory=dir)
        class MyTask(dj.Task):
            computations = 0
            @staticmethod
            def compute(datapoint, run_id):
                MyTask.computations += 1
                return datapoint["x"] + datapoint["y"]
            
            @staticmethod
            def get_dependencies(datapoint):
                return "x", "y"
        
        mygrid = dj.Frame().vary("x", [1,2,3])
        mygrid = mygrid.configure({"y":dj.Frame([9,8,7])})
        mygrid = mygrid.vary("y", [mygrid.select("y"), dj.run(operator.mul, mygrid.select("y"), 2)])

        MyTask.run(mygrid)

        

        mygrid.append({"x":100, "y":100})
        w = dj.Where(dj.run(operator.eq, mygrid.select("x"), 1))
        mygrid = w.false(mygrid)

        output = (MyTask.run(mygrid), MyTask.computations)

        should = (dj.run(operator.add, mygrid.select( "x"), mygrid.select("y")), 7)

        self.assertEqual(output, should)
        remove_folder(dir)

    
    def test_parents(self):
        dir = self.path + "parents"
        remove_folder(dir)

        @dj.AutoCache(directory=dir)
        class MyMultiplier(dj.Task):
            name = "multiplier"
            computations = 0
            @staticmethod
            def compute(datapoint, run_id):
                MyMultiplier.computations +=1
                return datapoint["a"] * datapoint["b"]
            
            @staticmethod
            def get_dependencies(datapoint):
                return "a", "b"

        @dj.AutoCache(directory=dir)
        class MyAdder(dj.Task):
            name = "adder"
            computations = 0
            parents = (MyMultiplier,)
            @staticmethod
            def compute(datapoint, run_id):
                MyAdder.computations += 1
                return datapoint["multiplier_output"] + datapoint["x"]
            
            @staticmethod
            def get_dependencies(datapoint):
                return "x", "multiplier_run_id"
        
        bla = dj.Frame([{"a":2, "b":3, "x":1}, {"a":3, "b":2, "x":1}])
        #MyAdder.run(bla)

        mygrid = dj.Frame([{"a":1, "b":1, "x":1}, {"a":1, "b":1, "x":2}, {"a":2, "b":3, "x":1}, {"a":3, "b":2, "x":1}])

        output = (MyAdder.run(mygrid), MyAdder.computations, MyMultiplier.computations)

        should = (dj.Frame([2,3,7,7]), 4, 3)

        self.assertEqual(output, should)
        remove_folder(dir)
    
    def test_nocache(self):
        dir = self.path + "nocache"
        remove_folder(dir)

        @dj.NoCache()
        class MyTask(dj.Task):
            computations = 0
            @staticmethod
            def compute(datapoint, run_id):
                MyTask.computations += 1
                return datapoint["x"] + datapoint["y"]
            
            @staticmethod
            def get_dependencies(datapoint):
                return "x", "y"
        
        mygrid = dj.Frame().vary( "x", [1,2,3])
        mygrid = mygrid.configure({"y":dj.Frame([9,8,7])})
        mygrid = mygrid.vary( "y", [mygrid.select( "y"), dj.run(operator.mul, mygrid.select( "y"), 2)])

        output = (MyTask.run(mygrid), MyTask.computations)

        should = (dj.run(operator.add, mygrid.select( "x"), mygrid.select( "y")), 6)

        self.assertEqual(output, should)
        remove_folder(dir)