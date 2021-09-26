import unittest
import datajuicer as dj
import datajuicer.errors as er

class TestUnique(unittest.TestCase):

    def test_data(self):
        frame1 = dj.vary(dj.Frame.new(),"key1" ,[1,1,2])

        u = dj.Unique(frame1)

        frame1 = u.data

        frame2 = dj.vary(dj.Frame.new(),"key1" ,[1,2])

        self.assertEqual(frame1, frame2)
    
    def test_is_canonical(self):
        frame1 = dj.vary(dj.Frame.new(),"key1" ,[1,1,2])

        u = dj.Unique(frame1)

        should = dj.Frame([True,False,True])

        self.assertEqual(u.is_canonical, should)
    
    def test_expand(self):
        frame1 = dj.vary(dj.Frame.new(),"key1" ,[1,1,2])

        u = dj.Unique(frame1)

        frame2 = dj.Frame(["a", "b"])

        should = dj.Frame(["a", "a", "b"])
        self.assertEqual(should, u.expand(frame2))