import datajuicer as dj
import sqlite3
from datajuicer.database import prepare_document, BaseDatabase
import time
import os
import json
import collections

def _format(val):
    if type(val) is str:
        return f"'{val}'"
    else:
        return str(val)

class FastSQLiteDB(BaseDatabase):

    def __init__(self, record_directory = "."):
        self.directory = record_directory
        self.record_path = os.path.join(record_directory, "runs.db")

        if not os.path.isdir(record_directory):
            os.makedirs(record_directory)

        if not os.path.exists(self.record_path):
            file = open(self.record_path, 'w+')
            file.close()
    
    def record_run(self, func_name, run_id, kwargs):
        
        document = prepare_document(func_name,kwargs,False)
        document["run_id"] = run_id
        document["start_time"] = int(time.time()*1000)
        document["done"] = False
        document["run_data"] = json.dumps(document)
        document = flatten_document(document)

        columns = [name for (_,name,_,_,_,_) in self._execute_command(f"PRAGMA table_info('{func_name}');", func_name) ]


        for key in document:
            if not key in columns:
                try:
                    self._execute_command(f'''ALTER TABLE '{func_name}' ADD COLUMN "{key}";''', func_name)
                except sqlite3.Error:
                    pass
        
        keys = ", ".join([f'"{key}"' for key in document.keys()])
        self._execute_command(f'''INSERT INTO '{func_name}' ({keys}) VALUES({", ".join(map(_format, document.values()))})''', func_name)

    def record_done(self, func_name, run_id):
        self._execute_command(f"UPDATE '{func_name}' SET done = True WHERE run_id = '{run_id}'", func_name)
    
    def get_raw(self, func_name):
        return [json.loads(rdata) for (rdata,) in self._execute_command(f"SELECT run_data FROM {func_name}", func_name)]

    def get_all_runs(self, func_name):
        return [rid for (rid,) in self._execute_command(f"SELECT run_id FROM {func_name}", func_name)]
    
    def get_newest_run(self, func_name, kwargs):

        document = flatten_document(prepare_document(func_name,kwargs,True))

        columns = [name for (_,name,_,_,_,_) in self._execute_command(f"PRAGMA table_info('{func_name}');", func_name) ]

        if not set(document.keys()).issubset(columns):
            return None

        conditions = [f'''"{key}" = {_format(value)}''' for (key,value) in document.items()]

        select = f"SELECT run_id FROM '{func_name}' WHERE {' AND '.join(conditions)} ORDER BY start_time DESC"

        results = self._execute_command(select, func_name)

        if len(results) == 0:
            return None
        return results[0][0]




    def delete_runs(self, func_name, run_ids):
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


def flatten_document(document):
    if document is dj.Ignore:
        return {}
    
    out = {}
    if type(document) is dict:
        for key, val in document.items():
            if type(val) in [int, float, bool, str]:
                out[key] = val
                continue
            flatval = flatten_document(val)
            for k2, v2 in flatval.items():
                out[key + "_" + k2] = v2
        
        return out
    
    if type(document) is list:
        for i, val in enumerate(document):
            if type(val) in [int, float, bool, str]:
                out["[" + str(i) + "]"] = val
                continue
            flatval = flatten_document(val)
            for k, v in flatval.items():
                out["[" + str(i) + "]_" + k] = v

        return out
