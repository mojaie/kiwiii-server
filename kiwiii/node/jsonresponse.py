#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import time

from tornado import gen
from kiwiii.task import Node


class JSONResponse(Node):
    def __init__(self, in_edge, task, params=None):
        super().__init__()
        self.in_edge = in_edge
        self.params = params
        self.response = {
            "id": task.id,
            "name": task.id[:8],
            "columns": [],
            "records": [],
            "created": time.strftime("%X %x %Z", time.localtime(task.created)),
            "status": "Completed"
        }

    def run(self):
        self.response["records"] = self.in_edge.records
        self.status = "done"

    def in_edges(self):
        return (self.in_edge,)


class AsyncJSONResponse(Node):
    def __init__(self, in_edge, task, params=None):
        super().__init__()
        self.in_edge = in_edge
        self.params = params
        self.response = {
            "id": task.id,
            "name": task.id[:8],
            "columns": [],
            "records": [],
            "created": time.strftime("%X %x %Z", time.localtime(task.created)),
            "status": "Completed"
        }

    @gen.coroutine
    def run(self):
        while self.in_edge.status != "done":
            in_record = yield self.in_edge.get()
            self.response["records"].append(in_record)
        self.status = "done"

    def in_edges(self):
        return (self.in_edge,)
