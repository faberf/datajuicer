import unittest
import datajuicer as dj
import datajuicer.errors as er
import pickle

class TestDocuments(unittest.TestCase):

    def test_prepare_document_output(self):


        doc = dj.database.prepare_document("myfunc", {"x":1, "y": 2, "hello":"greetings", "_arg_":list, "bla":dj.Ignore}, False)

        should = {
            'arg_x': 1,
            'arg_y': 2,
            'arg_hello': "str_greetings",
            'arg__arg_': f"hash_{hash(pickle.dumps(list))}",
            'arg_bla' : f"hash_{hash(pickle.dumps(dj.Ignore))}",
            'func_name':"myfunc"
        }
        self.assertEqual(doc, should)

    def test_prepare_document_output_keep_ignores(self):
        doc = dj.database.prepare_document("myfunc", {"x":1, "y": 2, "hello":"greetings", "_arg_":list, "bla":dj.Ignore}, True)

        should = {
            'arg_x': 1,
            'arg_y': 2,
            'arg_hello': "str_greetings",
            'arg__arg_': f"hash_{hash(pickle.dumps(list))}",
            'arg_bla': dj.Ignore,
            'func_name':"myfunc"
        }
        self.assertEqual(doc, should)