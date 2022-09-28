

from datajuicer.cache.equality_query import Matches
from datajuicer.core.run import Run
from datajuicer.utils import make_id


class NoRuns:
    pass

def find(cache, function, params_query, acceptable_states, return_all=False):
    query = Matches(dict(
        func = function,
        params = params_query
    ))

    for run in cache.search(query, sort_key="start_time"):
        if run.get_state() in acceptable_states:
            if return_all:
                yield run
            else:
                return run
    
    if not return_all:
        return NoRuns