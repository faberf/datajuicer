import os
from datajuicer.cache.query import Ignore
from datajuicer.ipc.lock import Lock


class DontSort:
    pass


class Cache:
    """Abstract cache class. This class is used to store the results of tasks. This is done so that the user does not have to recompute the results of tasks that have already been computed. For more information see Query and Document
    """

    def __init__(self, directory):
        self.directory = directory
    
    def get_lock(self, name):
        """Get a lock that is shared by all sessions connected to the same cache.

        Args:
            name (str): name of the lock.

        Returns:
            lock (Lock): the lock.
        """
        return Lock(os.path.join(self.directory, "locks"), name)
    
    def load_from_disk(self):
        """Load the cache from disk. This can be used to import runs from another machine.
        """
        raise Exception
    
    def get_hash(self):
        """Get a hash of the cache. This hash is used to check if the cache has changed since the last time it was used.

        Returns:
            hash (int): the hash.
        """
        return hash(tuple([doc["id"] for doc in self.search(Ignore())]))

    def search(self, query, sort_key=DontSort):
        """Search the cache for documents that match the query.

        Args:
            query (Query): the query.
            sort_key (str, callable, optional): the key to sort the results by. Can also be a function for custom sorting. Defaults to DontSort.
        Returns:
            results (list<dict>): the results.
        """
        raise Exception

    def delete(self, query, last_hash = None):
        """Delete documents from the cache that match the query. Also deletes the documents from disk.

        Args:
            query (Query): the query.
            last_hash (int, optional): the hash of the cache before the delete operation. If the hash of the cache has changed since the last time it was used, the delete operation will fail. Defaults to None.

        Raises:
            InvalidHashException: if the hash of the cache has changed since the last time it was used.
        
        Returns:
            ids (list<str>): the ids of the deleted documents.
        
        """
        raise Exception

    def insert(self, fields, last_hash=None):
        """ Insert new document into the cache.

        Args:
            fields (dict): the fields of the document.
            last_hash (int, optional): the hash of the cache before the insert operation. If None, the insert operation will always succeed. If the hash of the cache has changed since the last time it was used, the insert operation will fail. Defaults to None.

        Raises:
            InvalidHashException: if the hash of the cache has changed since the last time it was used.
        """
        raise Exception

    def update(self, query, fields, last_hash=None):
        """
        Update documents in the cache that match the query.

        Args:
            query (Query): the query.
            fields (dict): the fields to update.
            last_hash (int, optional): the hash of the cache before the update operation. If the hash of the cache has changed since the last time it was used, the update operation will fail. Defaults to None.

        Raises:
            InvalidHashException: if the hash of the cache has changed since the last time it was used.
        """
        raise Exception

