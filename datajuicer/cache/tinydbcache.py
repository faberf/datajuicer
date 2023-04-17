

import shutil
import tinydb, os
import dill as pickle
from datajuicer.cache.cache import Cache, DontSort
from datajuicer.cache.document import to_doc
from datajuicer.cache.query import Query
from datajuicer.errors import InvalidHashException, NoIDException
from datajuicer.ipc.lock import Lock
from datajuicer.utils import int_to_string, make_dir, string_to_int
import jsonpickle as jpickle
import ujson as json

class TinyDBQuery(tinydb.Query):
    def test(self, func, *args):
        return self._generate_test(
            lambda value: func(value, *args),
            ('test', self._path, func, args),
            allow_empty_path=True
        )

def check(query):
    def _check(doc):
        return query.check(unflatten(doc))
    return _check

def flatten(obj):
    p = jpickle.Pickler()
    return p.flatten(obj)

def unflatten(obj):
    p = jpickle.Unpickler()
    return p.restore(dict(obj))

class TinyDBCache(Cache):
    """TinyDB implementation of the Cache class.
    """
    def __init__(self, directory):
        super().__init__(directory)
        self.lock = self.get_lock("cache")
    
    def _get_db(self):
        return tinydb.TinyDB(os.path.join(self.directory, "runs.db"))
    
    def _get_hash(self):
        db = self._get_db()
        return hash(tuple([doc["id"] for doc in db.all()]))
    
    def get_hash(self):
        with self.lock:
            return self._get_hash()
    
    def load_from_disk(self):
        with self.lock:
            db = self._get_db()
            dirs =  [d for d in os.listdir(self.directory) if os.path.isdir(d) and not d in ["locks", "tmp"]]
            for d in dirs:
                path = os.path.join(self.directory, d, "doc.json")
                if not os.path.exists(path):
                    continue
                with open(path, "rb") as f:
                    fields = unflatten(json.load(f)) 
                    tinydoc = tinydb.Document(fields, doc_id=string_to_int(fields["id"]))
                    db.upsert(tinydoc)
        

            

    def search(self, query, sort_key = DontSort):

        with self.lock:
            db = self._get_db() #TODO: Do I need a lock here? race conditions?
            if type(query) is str:
                return unflatten(db.get(doc_id=string_to_int(query)))
            ret = []
            for doc in db.search(TinyDBQuery().test(check(query))):
                ret.append(unflatten(doc))

            if not sort_key is DontSort:
                key_func = sort_key
                if not callable(sort_key):
                    key_func = lambda doc: doc[sort_key]
                ret.sort(key = key_func)
            return ret

    def delete(self, query, last_hash=None):
        with self.lock:
            if not last_hash is None:
                if not self._get_hash()  == last_hash:
                    raise InvalidHashException
            db = self._get_db()
            if type(query) is str:
                db.remove([string_to_int(query)])
                shutil.rmtree(os.path.join(self.directory, query))
                return query
            ids = [int_to_string(doc_id) for doc_id in db.remove(TinyDBQuery().test(check(query)))]
            for id in ids:
                shutil.rmtree(os.path.join(self.directory, id))
            return ids

    def insert(self, fields, last_hash=None):
        if not "id" in fields:
            raise NoIDException
        with self.lock:
            if not last_hash is None:
                if not self._get_hash()  == last_hash:
                    raise InvalidHashException
            db = self._get_db()
            doc = flatten(to_doc(fields))
            path = os.path.join(self.directory, fields["id"], "doc.json")
            make_dir(path)
            with open(path, "w+") as f:
                json.dump(doc, f)
            db.insert(tinydb.table.Document(doc, doc_id=string_to_int(fields["id"])))

    def update(self, query, fields, last_hash=None):
        with self.lock:
            if not last_hash is None:
                if not self._get_hash()  == last_hash:
                    raise InvalidHashException
            
            doc = flatten(to_doc(fields))
            db = self._get_db()
            if type(query) is str:
                db.update(doc, doc_ids=[string_to_int(query)])
                docs = [dict(db.get(doc_id = string_to_int(query)))]
            else:
                cond = TinyDBQuery().test(check(query))
                ids = db.update(fields, cond = cond)
                docs = [db.get(id) for id in ids]
            for doc in docs:
                with open(os.path.join(self.directory, doc["id"], "doc.json"), "w+") as f:
                    json.dump(doc, f)
    
    
