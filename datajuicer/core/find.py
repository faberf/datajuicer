

from datajuicer.cache.equality_query import Matches
from datajuicer.core.run import Run
from datajuicer.utils import make_id


class NoRuns:
    pass

def find(cache, function, params_query, acceptable_states, return_all=False):
    
    def find_all():
        for run in cache.search(query, sort_key="start_time"):
            if run.get_state() in acceptable_states:
                yield run
    
    query = Matches(dict(
        func = function,
        params = params_query
    ))

    if return_all:
        return find_all()
    
    for doc in cache.search(query, sort_key="start_time"):
        run = Run(doc["id"], cache, doc["func"])
        if run.get_state() in acceptable_states:
            return run
    
    if not return_all:
        return NoRuns

