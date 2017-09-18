#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import time
import uuid

from tornado import gen
from kiwiii.node.node import Node


class JSONResponse(Node):
    def __init__(self, in_edge, params=None):
        self.in_edge = in_edge
        self.params = params
        id_ = str(uuid.uuid4())
        self.response = {
            "id": id_,
            "name": id_[:8],
            "columns": [],
            "records": [],
            "created": time.strftime("%X %x %Z"),
            "status": "Completed"
        }

    def run(self):
        self.response["records"] = self.in_edge.records

    def in_edges(self):
        return (self.in_edge,)


class AsyncJSONResponse(Node):
    def __init__(self, in_edge, params=None):
        self.in_edge = in_edge
        self.params = params
        id_ = str(uuid.uuid4())
        self.response = {
            "id": id_,
            "name": id_[:8],
            "columns": [],
            "records": [],
            "created": time.strftime("%X %x %Z"),
            "status": "Completed"
        }

    @gen.coroutine
    def run(self):
        while self.in_edge.status != "done":
            in_record = yield self.in_edge.get()
            self.response["records"].append(in_record)

    def in_edges(self):
        return (self.in_edge,)
