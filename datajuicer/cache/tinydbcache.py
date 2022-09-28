

import shutil
import tinydb, os
import dill as pickle
from datajuicer.cache.cache import Cache, DontSort
from datajuicer.cache.document import to_doc
from datajuicer.cache.query import Query
from datajuicer.errors import InvalidHashException, NoIDException
from datajuicer.ipc.lock import Lock
from datajuicer.utils import int_to_string, string_to_int


class TinyDBCache(Cache):
    
    def __init__(self, directory):
        super().__init__(directory)
        self.lock = self.get_lock("cache")
    
    def _get_db(self):
        return tinydb.TinyDB(os.path.join(self.directory, "runs.db"))
    
    def _get_hash(self):
        return super().get_hash()
    
    def get_hash(self):
        with self.lock:
            return self._get_hash()
    
    def load_from_disk(self):
        with self.lock:
            db = self._get_db()
            dirs =  [d for d in os.listdir(self.directory) if os.path.isdir(d) and not d in ["locks", "tmp"]]
            for d in dirs:
                path = os.path.join(self.directory, d, "doc.pickle")
                if not os.path.exists(path):
                    continue
                with open(path, "rb") as f:
                    fields = pickle.load(f).extract()
                    tinydoc = tinydb.Document(fields, doc_id=string_to_int(fields["id"]))
                    db.upsert(tinydoc)
        

            

    def search(self, query, sort_key = DontSort, return_all = False):
        with self.lock:

            db = self._get_db()
            if type(query) is str:
                return db.get(doc_id=string_to_int(query))
            ret = []
            for doc in db.search(tinydb.Query().test(query.check)):
                ret.append(doc)

        if not sort_key is DontSort:
            key_func = sort_key
            if not callable(sort_key):
                key_func = lambda doc: doc[sort_key]
            ret = sorted(ret, key_func)

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
            ids = [int_to_string(doc_id) for doc_id in db.remove(tinydb.Query().test(query.check))]
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
            doc = to_doc(fields)
            db.insert(doc.extract())
            with open(os.path.join(self.directory, fields["id"], "doc.pickle"), "wb+") as f:
                pickle.dump(doc, f)

    def update(self, query, fields, last_hash=None):
        with self.lock:
            if not last_hash is None:
                if not self._get_hash()  == last_hash:
                    raise InvalidHashException
            db = self._get_db()
            if type(query) is str:
                db.update(to_doc(fields).extract(), doc_ids=[string_to_int(id)])
                docs = [to_doc(db.get(doc_id = string_to_int(query)))]
            else:
                cond = tinydb.Query().test(query.check)
                ids = db.update(fields, cond = cond)
                docs = [to_doc(db.get(id)) for id in ids]
            for doc in docs:
                with open(os.path.join(self.directory, int_to_string(doc["id"]), "doc.pickle"), "wb+") as f:
                    pickle.dump(doc, f)
    
    
