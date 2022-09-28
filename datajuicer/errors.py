

class ReenterException(Exception):
    pass

class TimeoutException(Exception):
    pass

class IllegalWriteException(Exception):
    pass

class UnextractableQueryException(Exception):
    pass

class NoIDException(Exception):
    pass


class  InvalidHashException(Exception):
    pass

class NoResultException(Exception):
    pass

class NoActiveRunException(Exception):
    pass

class AlreadySetException(Exception):
    pass

class PrematureJoinException(Exception):
    pass