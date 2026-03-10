import os
import base64

def decodificacao(encoded_key):
    missing_padding = len(encoded_key) % 4
    if missing_padding != 0:
        encoded_key += '=' * (4 - missing_padding)
    KEY_JSON = base64.b64decode(encoded_key).decode('utf-8')
    return KEY_JSON