__version__ = "0.0.1"

import random
from base64 import b64encode
from datetime import datetime, timedelta
from hashlib import sha1
from time import time


def get_wsse(username, api_key):
    created = datetime.now().isoformat()
    b_nonce = sha1(str(random.random()).encode()).digest()
    b_digest = sha1(b_nonce + created.encode() + api_key.encode()).digest()

    return f'UsernameToken Username="{username}", PasswordDigest="{b64encode(b_digest).decode()}", Nonce="{b64encode(b_nonce).decode()}", Created="{created}"'
    return
