from datetime import datetime
from flask import jsonify


class StandardError(Exception):
    def __init__(self, status_code, type, message, payload=None):
        Exception.__init__(self)
        self.status_code = status_code
        self.date = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        self.type = type
        self.message = message
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        rv["type"] = self.type
        return rv
