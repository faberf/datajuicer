

from datajuicer.cache.equality_query import Matches
from datajuicer.core.run import Run
from datajuicer.utils import make_id


class NoRuns:
    pass

def find(cache, function, params_query, acceptable_states, return_all=False, sort_oldest=False):
    """Find a suitable run in the cache. If a suitable run is found, the run is loaded. If multiple suitable runs are found, the most recent one is loaded.

    Args:
        cache (Cache): the cache to search
        function (ipc.Function): the function to search for
        params_query (Query, dict): the parameters to search for
        acceptable_states (list): the acceptable states of the run, for example [RunState.Complete, RunState.Alive, RunState.Pending]
        return_all (bool, optional): if True, return all matching runs as a generator. Otherwise return a single matching run. Defaults to False.
        sort_oldest (bool, optional): if True, sort the runs from oldest to newest. Otherwise sort from newest to oldest. Defaults to False.

    Returns:
        run (Run): the run object

    Yields:
        run (Run): the run object
    """
    
    sort_key = "start_time"
    if sort_oldest:
        sort_key = lambda doc: -doc["start_time"]
    
    def find_all():
        for doc in cache.search(query, sort_key=sort_key):
            run = Run(doc["id"], cache, doc["func"])
            if run.get_state() in acceptable_states:
                yield run
    
    query = Matches(dict(
        func = function,
        params = params_query
    ))

    if return_all:
        return find_all()
    
    for doc in cache.search(query, sort_key=sort_key):
        run = Run(doc["id"], cache, doc["func"])
        if run.get_state() in acceptable_states:
            return run
    
    if not return_all:
        return NoRuns

