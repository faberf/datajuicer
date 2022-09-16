import string
import random

ID_LEN = 10

def rand_id():
    state = random.getstate()
    random.seed()
    letters = string.ascii_letters + string.digits
    ret = ''.join(random.choice(letters) for i in range(ID_LEN))

    random.setstate(state)
    return ret