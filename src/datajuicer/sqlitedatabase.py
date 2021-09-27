import datajuicer as dj
import sqlite3
from datajuicer.database import prepare_document, BaseDatabase
import time
import os
import json

class SQLiteDB(BaseDatabase):

    def __init__(self, record_directory = "."):
        self.record_path = os.path.join(record_directory, "runs.db")

        if not os.path.isdir(record_directory):
            os.makedirs(record_directory)

        if not os.path.exists(self.record_path):
            file = open(self.record_path, 'w+')
            file.close()
    
    def record_run(self, run_id, func, *args, **kwargs):
        document = prepare_document(func,args,kwargs,False)
        document["run_id"] = run_id
        document["start_time"] = int(time.time()*1000)
        document["done"] = False

        create_table = "CREATE TABLE IF NOT EXISTS Runs (run_id PRIMARY KEY, run_data);"

        insert = f'''INSERT INTO Runs (run_id, run_data) VALUES('{run_id}', '{json.dumps(document)}')'''

        try:
            conn = sqlite3.connect(self.record_path, timeout=100)
            c = conn.cursor()
            c.execute(create_table)
            c.execute(insert)
            conn.commit()
            c.close()
        except sqlite3.Error as error:
            raise error
        finally:
            if (conn):
                conn.close()

    def record_done(self, run_id):
        try:
            conn = sqlite3.connect(self.record_path, timeout=100)
            command = f"SELECT run_data FROM Runs WHERE run_id = '{run_id}'"
            cur = conn.cursor()
            cur.execute(command)
            doc = json.loads(cur.fetchall()[0][0])
            conn.commit()
            cur.close()
        except sqlite3.Error as error:
            raise error
        finally:
            if (conn):
                conn.close()
        
        doc["done"] = True
        update = f"UPDATE Runs SET run_data = '{json.dumps(doc)}' WHERE run_id = '{run_id}'"

        try:
            conn = sqlite3.connect(self.record_path, timeout=100)
            cur = conn.cursor()
            cur.execute(update)
            conn.commit()
            cur.close()
        except sqlite3.Error as error:
            raise error
        finally:
            if (conn):
                conn.close()
    
    def get_raw(self):
        try:
            conn = sqlite3.connect(self.record_path, timeout=100)
            command = f"SELECT run_data FROM Runs"
            cur = conn.cursor()
            cur.execute(command)
            all_runs = [json.loads(rdata) for (rdata,) in cur.fetchall()]
            conn.commit()
            cur.close()
        except sqlite3.Error as error:
            raise error
        finally:
            if (conn):
                conn.close()
        
        return all_runs

    def get_all_runs(self, func=None):
        all_all_runs = self.get_raw()
        
        if not func:
            return [run["run_id"] for run in all_all_runs]

        if type(func) is str:
            func_name = func
        if callable(func):
            if not type(func) is dj.Recordable:
                func = dj.Recordable(func)
            func_name  = func.name
        
        all_runs = []
        for rdata in all_all_runs:
            if rdata["func_name"] == func_name:
                all_runs.append(rdata["run_id"])
        
        return all_runs
    
    def get_newest_run(self, func, *args, **kwargs):

        document = prepare_document(func,args,kwargs,True)

        all_runs = self.get_raw()

        cur_start_time = -1

        def matches(rdata, search):
            output = True
            if search is dj.Ignore:
                return output

            if type(search) in [list, tuple]:
                for i, item in enumerate(search):
                    output &= matches(rdata[i], item)
                return output

            if type(search) is dict:
                for key, val in search.items():
                    if not key in rdata:
                        return False
                    output &= matches(rdata[key], val)
                return output
            
            return rdata == search

        ret = None
        for rdata in all_runs:
            if rdata["start_time"] > cur_start_time:
                if matches(rdata, document):
                    cur_start_time = rdata["start_time"]
                    ret = rdata
        
        return ret["run_id"]
    

    def delete_runs(self, run_ids):

        
        try:
            conn = sqlite3.connect(self.record_path, timeout=100)
            cur = conn.cursor()
            for rid in run_ids:
                command = f"DELETE FROM Runs WHERE run_id = '{rid}';"
                cur.execute(command)
            conn.commit()
            cur.close()
        except sqlite3.Error as error:
            raise error
        finally:
            if (conn):
                conn.close()
        
        
        if (conn):
            conn.close()