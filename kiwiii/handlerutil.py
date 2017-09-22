#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import base64
import csv
import functools
import os
import time

from kiwiii import static


class TemporaryDataStore(object):
    """Temporary data store for calculation result fetcher"""
    def __init__(self):
        self.container = []
        self.max_age = 86400 * 7  # Time(sec)

    def register(self, data, now=time.time()):
        # remove expired data
        alive = []
        for d in self.container:
            t = time.mktime(
                time.strptime(d["created"], "%X %x %Z"))
            if t + self.max_age > now:
                alive.append(d)
        self.container = alive
        # add new data
        self.container.append(data)

    def get(self, id_):
        for d in self.container:
            if d["id"] == id_:
                return d
        raise KeyError('Table not found')


def session_auth(method):
    """ Auth without redirect (for async request)"""
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if not self.current_user:
            def reject(self, *args, **kwargs):
                self.write({"authenticated": False})
            return reject(self, *args, **kwargs)
        return method(self, *args, **kwargs)
    return wrapper


def basic_auth(method):
    @functools.wraps(method)
    def wrapper(handler, *args, **kwargs):
        def reject(hdl):
            hdl.set_header(
                'WWW-Authenticate',
                'Basic realm={}'.format(static.BASIC_AUTH_REALM)
            )
            hdl.set_status(401)

        auth = handler.request.headers.get('Authorization')
        if auth is None:
            return reject(handler)
        if not auth.startswith('Basic '):
            return reject(handler)
        auth_decoded = base64.b64decode(auth[6:]).decode('utf-8')
        user, passwd = auth_decoded.split(':')

        if static.user_passwd_matched(user, passwd):
            return method(handler, *args, **kwargs)
        return reject(handler)
    return wrapper


class TemplateMatcher(object):
    def __init__(self, tmpl_file, name, limit=float("inf")):
        self.name = name
        self.columns = []
        self.template = {}  # {id1: {id: id1, col: val1}, ...}
        tpath = os.path.join(static.REPORT_TEMPLATE_DIR, tmpl_file)
        with open(tpath, newline="") as f:
            reader = csv.reader(f)
            header = next(reader)
            for i, row in enumerate(reader):
                if i + 1 > limit:
                    break
                d = dict(zip(header, row))
                d["idx"] = i
                self.template[d[header[0]]] = d
            for h in header:
                self.columns.append({"key": h, "visible": True})

    def add_array(self, array, name):
        for k, v in array:
            if k in self.template:
                self.template[k][name] = v
        self.columns.append({"key": name, "visible": True})

    def to_json(self):
        records = sorted(self.template.values(), key=lambda x: x["idx"])
        return {
            "name": self.name,
            "columns": self.columns,
            "records": records
        }
