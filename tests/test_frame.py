from typing import Type
from datajuicer.interface import configure
import unittest
import datajuicer as dj
import datajuicer.errors as er

class TestFrames(unittest.TestCase):

    def test_equality_of_new_frames(self):
        frame1 = dj.Frame.new()

        frame2 = dj.Frame.new()

        self.assertEqual(frame1, frame2)

    
    def test_value_of_new_frame(self):

        frame1 = dj.Frame.new()

        frame3 = dj.Frame([{}])

        self.assertEqual(frame1, frame3)   
    
    def test_configure_range_error(self):

        frame1 = dj.Frame.new()

        frame4 = dj.Frame([1,2,3])

        self.assertRaises(er.RangeError, dj.configure, frame1, {"key":frame4})
    
    def test_configure_with_list_as_value(self):

        frame1 = dj.Frame.new()

        frame5 = dj.configure(frame1, {"key":[1,2,3]})

        self.assertEqual(frame5, dj.Frame([{"key":[1,2,3]}]))
    
    def test_empty_configure(self):

        frame1 = dj.Frame.new()

        self.assertEqual(frame1, configure(frame1, {}))
    
    def test_vary_value_type_error(self):

        self.assertRaises(TypeError, dj.vary, dj.Frame.new(), "key", "a")

    def test_vary_output_len(self):
        frame1 = dj.vary(dj.Frame.new(),"key1" ,[1,2,3])

        self.assertEqual(len(frame1), 3)
    
    def test_vary_range_error(self):

        frame2 = dj.Frame([[1,2,3,4], [5,6], [7]])

        self.assertRaises(er.RangeError, dj.vary, dj.Frame.new(), "key2",frame2)
    
    def test_vary_with_values_frame_output(self):

        frame1 = dj.vary(dj.Frame.new(),"key1" ,[1,2,3])

        frame2 = dj.Frame([[1,2,3,4], [5,6], [7]])

        frame3 = dj.vary(frame1, "key2", frame2)

        should = dj.Frame([{'key1': 1, 'key2': 1}, {'key1': 1, 'key2': 2}, {'key1': 1, 'key2': 3}, {'key1': 1, 'key2': 4}, {'key1': 2, 'key2': 5}, {'key1': 2, 'key2': 6}, {'key1': 3, 'key2': 7}])

        self.assertEqual(frame3, should)
    
    def test_vary_with_key_frame(self):
        frame1 = dj.vary(dj.Frame.new(),"key1" ,[1,2,3])

        frame4 = dj.vary(frame1, dj.Frame(['1','2', '3']),[1])

        self.assertEqual(frame4, dj.Frame([{'key1': 1, '1': 1}, {'key1': 2, '2': 1}, {'key1': 3, '3': 1}]) )