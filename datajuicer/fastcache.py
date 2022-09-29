

# from datajuicer.cache import BaseCache, SimpleCache, make_dir
# import os
# import sqlite3
# from datajuicer.dependency import TaskNameDependency
# from datajuicer.run import Run

# def execute_command(command, conn, return_dicts = False):
#     def dict_factory(cursor, row):
#         d = {}
#         for idx, col in enumerate(cursor.description):
#             d[col[0]] = row[idx]
#         return d
#     if type(command) is str:
#         command = [command]
#     try:
#         cur = conn.cursor()
#         for com in command:
#             cur.execute(com)
#         out = cur.fetchall()
#         if return_dicts:
#             _out = []
#             for row in out:
#                 _out.append(dict_factory(cur, row))
#             out = _out
#         cur.close()
#     except sqlite3.Error as error:
#         conn.rollback()
#         raise error
    
#     return out

# def start_modify(db_file):
#     make_dir(db_file)
#     conn = sqlite3.connect(db_file, timeout=100)
#     cur = conn.cursor()
#     cur.execute("BEGIN EXCLUSIVE")
#     cur.close()
#     return conn
    

# class FastCache(BaseCache):
#     def __init__(self, dir_path) -> None:
#         self.dir_path = dir_path
#         self.simple_cache = SimpleCache(dir_path)
#         self.db_file = os.path.join(self.simple_cache.dir_path, "fastcache.db")
    
#     def up_to_date(self):
#         return set(self.fs_all_runs()) == set(self.db_all_runs())
    
#     def load(self):
#         conn = start_modify(self.db_file)
#         execute_command(f"CREATE TABLE IF NOT EXISTS 'runs' (run_id PRIMARY KEY, state, deps, start_time);", conn)

        


    
#     def db_task_names(self, conn):
#         try:
#             task_names =  execute_command(
#                 '''
#                 SELECT 
#                     name
#                 FROM 
#                     sqlite_master
#                 WHERE 
#                     type ='table' AND 
#                     name NOT LIKE 'sqlite_%';
#                 ''', conn)
#             return [tn[0] for tn in task_names]
#         except sqlite3.OperationalError as e:
#             return []

    
#     def db_all_runs(self):
#         conn = sqlite3.connect(self.db_file)
#         task_names = self.db_task_names(conn)
#         all_runs = []
#         for task_name in task_names:
#             all_runs += execute_command(f"SELECT run_id from '{task_name}'", conn)
#         conn.close()
#         return all_runs
    
#     def fs_all_runs(self):
#         set([f for f in os.listdir(self.dir_path) if os.path.isdir(os.path.join(self.dir_path, f))])