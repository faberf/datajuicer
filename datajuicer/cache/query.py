

from datajuicer.cache.document import to_doc
from datajuicer.errors import UnextractableQueryException



class Query:
    """Abstract query class. Queries can be composed. This class is used to query the cache. For more information see Cache and Document."""
    #TODO: explain how to use this class and how queries are composed.
    def extract(self):
        """Extract the value of the query. This is useful so that a new run can be started if no results are found in the cache.
        """
        raise Exception
    def to_document(self):
        return to_doc(self.extract())
    def check(self, document):
        """Check if a document matches the query.
        Args:
            document (Document): the document to check.
        Returns:
            match (bool): True if the document matches the query.
        """
        raise Exception

class Default:
    pass

class NoDocument:
    pass

class Any(Query):
    """A query that matches any document.
    """
    def __init__(self, default=Default):
        self.default = default
    
    def check(self, document = NoDocument):
        if document is NoDocument:
            return False 
        return True
    
    def extract(self):
        if self.default is Default:
            raise UnextractableQueryException("Cannot extract an 'Any' query without a default value.")
        return self.default

class Ignore(Query):
    """A query that matches any document or no document.
    """
    def __init__(self, default=Default):
        self.default = default
        
    def check(self, document = NoDocument):
        return True
    
    def extract(self):
        if self.default is Default: 
            raise UnextractableQueryException("Cannot extract an 'Ignore' query without a default value.")
        return self.default

# class DictQuery(Query):
#     def __init__(self, fields):
#         self.fields = fields
    
#     def extract(self):
#         return {key:val.extract() for key,val in self.fields()}

# class Matches(DictQuery):
    
#     def check(self, document = None):
#         if document is None:
#             return False
        
#         if not type(document) is DictDocument:
#             raise TypeError("The 'Matches' query only works for dictionaries.")
        
#         for key, val in self.fields.items():
#             if not val.check(document.fields.get(key, None)):
#                 return False
#         return True

# class Exactly(Query):
#     def __init__(self, fields):
#         raise NotImplementedError

class Between(Query):
    def __init__(self, start, stop, default=None):
        raise NotImplementedError

class Among(Query):
    def __init__(self, values, default=None):
        raise NotImplementedError

class Close(Query):
    def __init__(self, value, epsilon=None):
        raise NotImplementedError



