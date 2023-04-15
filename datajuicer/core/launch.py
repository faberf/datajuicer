
import sys, os 
import dill as pickle

""" This module is used to launch a run in a new process. It is the entry point for the new process. It is used by multiprocessing launchers.
"""

if __name__ == "__main__":
    sys.path.append(os.path.realpath('.'))


def load():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("-path")
    args = ap.parse_args()
    with open(args.path, "rb") as f:
        run = pickle.load(f)
    os.remove(args.path)
    return run

if __name__ == "__main__":
    load().execute()