import string
import random

def rand_id():
    letters = string.ascii_letters + string.digits
    return ''.join(random.choice(letters) for i in range(10))