import unittest
import datajuicer as dj
import datajuicer.errors as er

class TestVariables(unittest.TestCase):

    def test_variable(self):
        frame = dj.Frame().vary( "a", [4,2])

        frame = frame.vary( "b", [700,800])

        var = dj.Variable("a")

        output = (*var.iterate(frame),)

        should = ( (4, dj.Frame([{"a":4, "b":700}, {"a":4, "b":800}])), (2, dj.Frame([{"a":2, "b":700}, {"a":2, "b":800}])) )

        self.assertEqual(output, should)
    
    def test_product_variable(self):

        frame = dj.Frame([])

        frame.append({"a": 1, "b": 3})
        frame.append({"a": 2, "b": 3})
        frame.append({"a": 2, "b": 1})
        frame.append({"a": 2, "b": 2})

        frame = frame.vary("c", [5,6])

        var = dj.JointVariable(dj.Variable("a"), dj.Variable("b"))
        