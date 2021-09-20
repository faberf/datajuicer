import unittest
import datajuicer as dj
import datajuicer.errors as er
import pickle

class TestDocuments(unittest.TestCase):

    def test_prepare_document_output(self):
        def myfunc(x, y, hello="hi", _arg_= list, bla=5):
            pass


        doc = dj.database.prepare_document(myfunc, (1,2), {"hello":"greetings", "bla":dj.Ignore}, False, "myfunc")

        should = {
            'arg_x': 1,
            'arg_y': 2,
            'arg_hello': "greetings",
            'arg__arg_': pickle.dumps(list),
            'arg_bla' : pickle.dumps(dj.Ignore),
            'func_name':"myfunc"
        }
        self.assertEqual(doc, should)

    def test_prepare_document_output_keep_ignores(self):
        def myfunc(x, y, hello="hi", _arg_= list, bla=5):
            pass


        doc = dj.database.prepare_document(myfunc, (1,2), {"hello":"greetings", "bla":dj.Ignore}, True, "myfunc")

        should = {
            'arg_x': 1,
            'arg_y': 2,
            'arg_hello': "greetings",
            'arg__arg_': pickle.dumps(list),
            'arg_bla': dj.Ignore,
            'func_name':"myfunc"
        }
        self.assertEqual(doc, should)