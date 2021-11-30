import os.path
import sqlite3
import datajuicer
import datajuicer.cache as cache
import pickle
import time
import json

def _format(val):
    if type(val) is str:
        return f"'{val}'"
    else:
        return str(val)



def start_modify(db_file):
    cache.make_dir(db_file)
    conn = sqlite3.connect(db_file, timeout=100)
    cur = conn.cursor()
    cur.execute("BEGIN EXCLUSIVE")
    cur.close()
    return conn
    


def execute_command(command, conn, return_dicts = False):
    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d
    if type(command) is str:
        command = [command]
    try:
        cur = conn.cursor()
        for com in command:
            cur.execute(com)
        out = cur.fetchall()
        if return_dicts:
            _out = []
            for row in out:
                _out.append(dict_factory(cur, row))
            out = _out
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
                for i in range(len(key)-1):
                    if key[i:i+2] == "_e":
                        if key[i-1] != "_":
                            cur_key, key = key[:i], key[i+2:]
                            break
                #cur_key, key = key.split("_e",1)
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
        if cursor is not None and cur_key is not None:
            cursor[cur_key] = val
    return obj

                
              

class LocalCache(cache.BaseCache):
    def __init__(self, root = "dj_runs"):
        super().__init__()
        self.root = root
        self.db_file = os.path.join(self.root, "runs.db")
        #self.lock = threading.RLock()
    
    def transfer(self, other):
        return super().transfer(other)

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
            all_rows = execute_command(f"SELECT version, run_id from '{task_name}'", conn)
            for version, run_id in all_rows:
                yield task_name, version, run_id
        conn.close()
        


    def has_run(self, task_name, version, run_id):
        conn = sqlite3.connect(self.db_file)
        task_names = self._get_task_names(conn)
        if not task_name in task_names:
            return False
        res = execute_command(f'''SELECT EXISTS(SELECT 1 FROM '{task_name}' WHERE version={version} AND run_id="{run_id}");''', conn)
        conn.close(res)
        return res


    def get_newest_runs(self, task_name, version, matching):
        conn = start_modify(self.db_file)
        if not task_name in self._get_task_names(conn):
            return []
        raw_args = flatten(cache.make_raw_args(matching))
        columns = [name for (_,name,_,_,_,_) in execute_command(f"PRAGMA table_info('{task_name}');", conn) ]

        if not set(raw_args.keys()).issubset(columns):
            return []
        
        conditions = [f'''"{key}" = {_format(value)}''' for (key,value) in raw_args.items()] + [f'''"version" = {_format(version)} ''']

        select = f"SELECT run_id FROM '{task_name}' WHERE {' AND '.join(conditions)} ORDER BY start_time DESC"

        results = execute_command(select, conn)
        conn.close()
        return [res[0] for res in results]

    def is_done(self, task_name, version, run_id):
        conn = sqlite3.connect(self.db_file)
        if not task_name in self._get_task_names(conn):
            return False
        res = execute_command(f'''SELECT done FROM '{task_name}' WHERE version={version} AND run_id="{run_id}";''', conn)
        if res == []:
            return False
        conn.close()
        return res[0][0]

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
        execute_command(f'''INSERT OR IGNORE INTO '{task_name}' ({keys}) VALUES({", ".join(map(_format, raw_args.values()))})''', conn)


    def record_result(self, task_name, version, run_id, result):
        conn = start_modify(self.db_file)
        self._record_result(task_name, version, run_id, result, conn)
        conn.commit()
        conn.close()
    
    def _record_result(self, task_name, version, run_id, result, conn):
        path = os.path.join(self.root, run_id, "result.pickle")
        cache.make_dir(path)
        with open(path, "bw+") as f:
            pickle.dump(result, f)
        execute_command(f"UPDATE '{task_name}' SET done = True WHERE run_id = '{run_id}'", conn)


    def get_result(self, task_name, version, run_id):
        with open(os.path.join(self.root, run_id, "result.pickle"), "br") as f:
            return pickle.load(f)

    def open(self, task_name, version, run_id, path, mode):
        fullpath = os.path.join(self.root, run_id, "user_files",path)
        cache.make_dir(fullpath)
        return open(fullpath, mode)

    def copy_files(self, task_name, version, run_id):
        return cache.copy(os.path.join(self.root, run_id, "user_files"))

    def make_run(self, task_name, version, run_id, raw_args, start_time,result, files, run_deps):
        try:
            conn = start_modify(self.db_file)
            self._record_raw_args(task_name, version, run_id, raw_args, start_time, True, conn)
            self._record_result(task_name, version, run_id, result, conn)
            for run_dep in run_deps:
                self.add_run_dependency(task_name, version, run_id, run_dep[0], run_dep[1], run_dep[2])
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
        res = execute_command(f'''SELECT start_time FROM '{task_name}' WHERE version={version} AND run_id="{run_id}";''', conn)[0][0]
        conn.close()
        return res

    def get_raw_args(self, task_name, version, run_id):
        conn = sqlite3.connect(self.db_file)
        def dict_factory(cursor, row):
            d = {}
            for idx, col in enumerate(cursor.description):
                d[col[0]] = row[idx]
            return d
        d = execute_command(f'''SELECT * FROM '{task_name}' WHERE version={version} AND run_id="{run_id}";''', conn, return_dicts=True)[0]
        conn.close()
        return unflatten(d)
    
    def add_run_dependency(self, task_name, version, run_id, other_task_name, other_version, other_run_id):
        with open(os.path.join(self.root, run_id, "run_deps.json"), "w+") as f:
            try:
                run_deps = json.load(f)
            except json.JSONDecodeError:
                run_deps = []
            run_deps.append((other_task_name, other_version, other_run_id))
            f.seek(0)  # rewind
            json.dump(run_deps, f)
            f.truncate()

    def get_run_dependencies(self, task_name, version, run_id):
        path = os.path.join(self.root, run_id, "run_deps.json")
        if not os.path.exists(path):
            return []
        with open(path, "r") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return []

    
        