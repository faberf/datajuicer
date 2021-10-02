import datajuicer as dj
import sqlite3
from datajuicer.database import prepare_document, BaseDatabase
import time
import os
import json
import collections

class SmallSQLiteDB(BaseDatabase):

    def __init__(self, record_directory = "."):
        self.directory = record_directory
        self.record_path = os.path.join(record_directory, "runs.db")

        if not os.path.isdir(record_directory):
            os.makedirs(record_directory)

        if not os.path.exists(self.record_path):
            file = open(self.record_path, 'w+')
            file.close()
        
        
    
    def record_run(self, func, run_id, *args, **kwargs):
        if callable(func):
            if not type(func) is dj.Recordable:
                func = dj.Recordable(func)
            func_name = func.name
        else:
            raise TypeError
        document = prepare_document(func,args,kwargs,False)
        document["run_id"] = run_id
        document["start_time"] = int(time.time()*1000)
        document["done"] = False
        insert = f'''INSERT INTO '{func_name}' (run_id, run_data) VALUES('{run_id}', '{json.dumps(document)}')'''

        self._execute_command(insert, func_name)

    def record_done(self, func, run_id):
        if type(func) is str:
            func_name = func
        if callable(func):
            if not type(func) is dj.Recordable:
                func = dj.Recordable(func)
            func_name = func.name
        doc = json.loads(self._execute_command(f"SELECT run_data FROM '{func_name}' WHERE run_id = '{run_id}'", func_name)[0][0])
        
        doc["done"] = True
        self._execute_command(f"UPDATE '{func_name}' SET run_data = '{json.dumps(doc)}' WHERE run_id = '{run_id}'", func_name)
    
    def get_raw(self, func):
        if type(func) is str:
            func_name = func
        if callable(func):
            if not type(func) is dj.Recordable:
                func = dj.Recordable(func)
            func_name = func.name
        return [json.loads(rdata) for (rdata,) in self._execute_command(f"SELECT run_data FROM '{func_name}'", func_name)]

    def get_all_runs(self, func):
        return [run["run_id"] for run in self.get_raw(func)]
    
    def get_newest_run(self, func, *args, **kwargs):

        document = prepare_document(func,args,kwargs,True)

        all_runs = self.get_raw(func)

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
                    ret = rdata["run_id"]
        return ret


    def delete_runs(self, func, run_ids):
        if type(func) is str:
            func_name = func
        if callable(func):
            if not type(func) is dj.Recordable:
                func = dj.Recordable(func)
            func_name = func.name

        self._execute_command([f"DELETE FROM '{func_name}' WHERE run_id = '{rid}';" for rid in run_ids], func_name)

    def _execute_command(self, command, func_name):
        if type(command) is str:
            command = [command]
        command = [f"CREATE TABLE IF NOT EXISTS '{func_name}' (run_id PRIMARY KEY, run_data);"] + command
        try:
            conn = sqlite3.connect(self.record_path, timeout=100)
            cur = conn.cursor()
            for com in command:
                cur.execute(com)
            out = cur.fetchall()
            conn.commit()
            cur.close()
        except sqlite3.Error as error:
            raise error
        finally:
            if (conn):
                conn.close()
        
        return out