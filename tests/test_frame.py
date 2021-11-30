import unittest
import datajuicer as dj
import datajuicer.errors as er

class TestFrames(unittest.TestCase):

    def test_value_of_new_frame(self):

        frame1 = dj.Frame()

        frame3 = dj.Frame([{}])

        self.assertEqual(list(frame1), list(frame3))   
    
    def test_configure_range_error(self):

        frame1 = dj.Frame()

        frame4 = dj.Frame([1,2,3])

        self.assertRaises(er.RangeError, frame1.configure, {"key":frame4})
    
    def test_configure_with_list_as_value(self):

        f1 = dj.Frame()

        f2 = dj.Frame(f1).configure({"key":[1,2,3]})

        self.assertEqual(list(f2), [{"key":[1,2,3]}])
    
    def test_configure_basic(self):
        f = dj.Frame()
        f.configure({"attribute":"value"})
        self.assertEqual(list(f), [{"attribute": "value"}])
    
    def test_configure_multiple(self):
        f = dj.Frame()
        f.configure({"one":1, "two":2})
        self.assertEqual(list(f), [{"one":1, "two":2}])
    
    def test_configure_direct(self):
        f = dj.Frame().configure({"one":1, "two":2})
        self.assertEqual(list(f), [{"one":1, "two":2}])
    
    def test_set_attr(self):
        f = dj.Frame()
        f["attribute"] = "value"
        self.assertEqual(list(f), [{"attribute": "value"}])
    
    def test_vary_basic(self):
        f = dj.Frame()
        f["att"] = dj.Vary([1, 2, 3])
        self.assertEqual(list(f), [{"att":1},{"att":2},{"att":3}])
    
    def test_vary_multiple(self):
        f = dj.Frame().configure({"a":dj.Vary([1,2]), "b":dj.Vary([1,2])})
        should = [{"a":1, "b":1}, {"a":1, "b":2}, {"a":2, "b":1}, {"a":2, "b":2}]
        self.assertEqual(list(f), should)
    
    def test_empty_configure(self):
        f = dj.Frame()
        f.configure({"attribute":"value"})
        f2 = dj.Frame(f).configure({})
        self.assertEqual(list(f), list(f2))
    
    def test_frame_as_value(self):
        f = dj.Frame([{},{}])
        f["a"] = dj.Frame([1,2])
        f["b"] = dj.Vary(dj.Frame([[1,2], [3]]))
        f["c"] = dj.Vary([1, dj.Frame([1,2,3])])

        should = [
            {"a":1, "b":1, "c":1},
            {"a":1, "b":1, "c":1},
            {"a":1, "b":2, "c":1},
            {"a":1, "b":2, "c":2},
            {"a":2, "b":3, "c":1},
            {"a":2, "b":3, "c":3}
        ]

        self.assertEqual(list(f), should)
    
    def test_vary_multiple_simul(self):
        f = dj.Frame()
        f.configure({"a":dj.Vary([1,2]), "b":dj.Vary([1,2])})
        should = [
            {"a":1, "b":1},
            {"a":1, "b":2},
            {"a":2, "b":1},
            {"a":2, "b":2}
        ]
        self.assertEqual(list(f), should)
    
    def test_select_basic(self):
        f= dj.Frame().configure({"a":dj.Vary([1,2]), "b":dj.Vary([1,2])})
        self.assertEqual(list(f["a"]), [1,1,2,2])
    
    def test_select_frame_input(self):
        f= dj.Frame().configure({"a":dj.Vary([1,2]), "b":dj.Vary([1,2])})
        keys = dj.Frame(["a", "b", "b", "a"])
        self.assertEqual(list(f[keys]), [1,2,1,2])
    
    def test_group_by_basic(self):
        f= dj.Frame().configure({"a":dj.Vary([1,2]), "b":dj.Vary([1,2])})
        f.where(f["a"] == 1)["c"] = dj.Vary([1,2,3])
        f.where(f["a"] == 2)["c"] = 4
        f.group_by("c")
        self.assertEqual(list(f), [{'c': 1, 'a': [1, 1], 'b': [1, 2]}, {'c': 2, 'a': [1, 1], 'b': [1, 2]}, {'c': 3, 'a': [1, 1], 'b': [1, 2]}, {'c': 4, 'a': [2, 2], 'b': [1, 2]}])
        
