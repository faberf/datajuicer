import os.path
import sqlite3
import datajuicer.cache as cache
import pickle
import time
import threading

def _format(val):
    if type(val) is str:
        return f"'{val}'"
    else:
        return str(val)

def start_modify(db_file):
    conn = sqlite3.connect(db_file, timeout=100)
    cur = conn.cursor()
    cur.execute("BEGIN EXCLUSIVE")
    cur.close()
    return conn
    


def execute_command(command, conn):
    if type(command) is str:
        command = [command]
    try:
        cur = conn.cursor()
        for com in command:
            cur.execute(com)
        out = cur.fetchall()
        cur.close()
    except sqlite3.Error as error:
        conn.rollback()
        raise error
    
    return out

def escape_underscore(str):
    return(str.replace("_", "__"))

def flatten(document):
    
    out = {}
    if type(document) is dict:
        for key, val in document.items():
            if type(val) in [int, float, bool, str]:
                out["_d" + escape_underscore(key) + "_e"] = val
                continue
            flatval = flatten(val)
            for k2, v2 in flatval.items():
                out["_d" + escape_underscore(key) + "_e" + k2] = v2
        
        return out
    
    if type(document) is list:
        for i, val in enumerate(document):
            if type(val) in [int, float, bool, str]:
                out[f"_l{i}_{len(document)}_e"] = val
                continue
            flatval = flatten(val)
            for k, v in flatval.items():
                out[f"_l{i}_{len(document)}_e{k}"] = v

        return out

       

def unflatten(dictionary):
    class NoDataYet:
        pass
    obj = None

    for key, val in dictionary.items():
        cur_key = None
        cursor = obj
        def key_in_obj(key, obj):
            if type(obj) is dict:
                return key in obj
            if type(obj) is list:
                return obj[key] != NoDataYet
        while key != "":
            if key.startswith("_d"):
                if cursor is None:
                    obj = {}
                    cursor = obj
                if not cur_key is None:
                    if not key_in_obj(cur_key, cursor):
                        cursor[cur_key] = {}
                    cursor = cursor[cur_key]
                key = key[2:]
                cur_key, key = key.split("_e",1)
                cur_key = cur_key.replace("__", "_")
            elif key.startswith("_l"):
                key = key[len("_l"):]
                stuff, key = key.split("_e",1)
                idx, length = stuff.split("_")
                length = int(length)
                if cursor is None:
                    obj = [NoDataYet] * length
                    cursor = obj
                if not cur_key is None:
                    if not key_in_obj(cur_key, cursor):
                        cursor[cur_key] = [NoDataYet] * length
                    cursor = cursor[cur_key]
                cur_key = int(idx)
            else:
                break
        cursor[cur_key] = val
    return obj

                
              

class LocalCache:
    def __init__(self, root = "dj_runs"):
        self.root = root
        self.db_file = os.path.join(self.root, "runs.db")
        #self.lock = threading.RLock()
    

    def _get_task_names(self, conn):
        task_names =  execute_command(
            '''
            SELECT 
                name
            FROM 
                sqlite_schema
            WHERE 
                type ='table' AND 
                name NOT LIKE 'sqlite_%';
            ''', conn)
        return [tn[0] for tn in task_names]

    def all_runs(self):
        conn = sqlite3.connect(self.db_file)
        task_names = self._get_task_names(conn)
        for task_name in task_names:
            all_rows = execute_command("SELECT version, run_id from '{task_name}'", conn)
            for version, run_id in all_rows:
                yield task_name, version, run_id
        conn.close()
        


    def has_run(self, task_name, version, run_id):
        conn = sqlite3.connect(self.db_file)
        task_names = self._get_task_names(conn)
        if not task_name in task_names:
            return False
        res = execute_command(f'''SELECT EXISTS(SELECT 1 FROM '{task_name}' WHERE version="{version}" AND run_id="{run_id}");''', conn)
        conn.close(res)
        return res


    def get_newest_run(self, task_name, version, matching):
        conn = start_modify(self.db_file)
        if not task_name in self._get_task_names(conn):
            return False
        raw_args = flatten(cache.make_raw_args(matching))
        columns = [name for (_,name,_,_,_,_) in execute_command(f"PRAGMA table_info('{task_name}');", conn) ]

        if not set(raw_args.keys()).issubset(columns):
            return None
        
        conditions = [f'''"{key}" = {_format(value)}''' for (key,value) in raw_args.items()] + [f'''"version" = {_format(version)} ''']

        select = f"SELECT run_id FROM '{task_name}' WHERE {' AND '.join(conditions)} ORDER BY start_time DESC"

        results = execute_command(select, conn)
        conn.close()
        if len(results) == 0:
            return None
        return results[0][0]

    def is_done(self, task_name, version, run_id):
        conn = sqlite3.connect(self.db_file)
        if not task_name in self._get_task_names():
            return False
        res = execute_command(f'''SELECT done FROM '{task_name}' WHERE version="{version}" AND run_id="{run_id}";''', conn)[0][0]
        conn.close()
        return res

    def record_run(self, task_name, version, run_id, kwargs):
        conn = start_modify(self.db_file)
        raw_args = cache.make_raw_args(kwargs)
        self._record_raw_args(task_name, version, run_id, raw_args, int(time.time()*1000), False, conn)
        conn.commit()
        conn.close()
    
    def _record_raw_args(self, task_name, version, run_id, raw_args, start_time, done, conn):
        if not task_name in self._get_task_names(conn):
            execute_command(f"CREATE TABLE IF NOT EXISTS '{task_name}' (run_id PRIMARY KEY, version, done, start_time);", conn)
        raw_args = flatten(raw_args)
        raw_args["run_id"] = run_id
        raw_args["version"] = version
        raw_args["start_time"] = start_time
        raw_args["done"] = done
        columns = [name for (_,name,_,_,_,_) in execute_command(f"PRAGMA table_info('{task_name}');", conn) ]

        for key in raw_args:
            if not key in columns:
                try:
                    execute_command(f'''ALTER TABLE '{task_name}' ADD COLUMN "{key}";''', conn)
                except sqlite3.Error:
                    pass
        
        keys = ", ".join([f'"{key}"' for key in raw_args.keys()])
        execute_command(f'''INSERT INTO '{task_name}' ({keys}) VALUES({", ".join(map(_format, raw_args.values()))})''', conn)


    def record_result(self, task_name, version, run_id, result):
        conn = start_modify(self.db_file)
        with open(os.path.join(self.root, run_id, "result.pickle"), "bw+") as f:
            pickle.dump(result, f)
        execute_command(f"UPDATE '{task_name}' SET done = True WHERE run_id = '{run_id}'", self.db_file)
        conn.commit()
        conn.close()

    def get_result(self, task_name, version, run_id):
        with open(os.path.join(self.root, run_id, "result.pickle"), "br") as f:
            return pickle.load(f)

    def open(self, task_name, version, run_id, path, mode):
        fullpath = os.path.join(self.root, run_id, "user_files",path)
        return open(fullpath, mode)

    def copy_files(self, task_name, version, run_id):
        return cache.copy(os.path.join(self.root, run_id, "user_files"))

    def make_run(self, task_name, version, run_id, raw_args, start_time,result, files):
        try:
            conn = start_modify(self.db_file)
            self._record_raw_args(task_name, version, run_id, raw_args, start_time, True, conn)
            self.record_result(task_name, version, run_id, result)
            cache.paste(files, os.path.join(self.root, run_id, "user_files"))
            conn.commit()
            conn.close()
        except Exception as e:
            if conn:
                conn.rollback()
                conn.close()
            raise e
    
    def get_start_time(self, task_name, version, run_id):
        conn = sqlite3.connect(self.db_file)
        res = execute_command(f'''SELECT start_time FROM '{task_name}' WHERE version="{version}" AND run_id="{run_id}";''', conn)[0][0]
        conn.close()
        return res

    def get_raw_args(self, task_name, version, run_id):
        conn = sqlite3.connect(self.db_file)
        d = dict(execute_command(f'''SELECT * FROM '{task_name}' WHERE version="{version}" AND run_id="{run_id}";''', conn)[0])
        conn.close()
        return unflatten(d)

    
        