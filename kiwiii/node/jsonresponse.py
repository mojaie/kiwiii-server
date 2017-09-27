#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import time

from tornado import gen
from kiwiii.task import Node, AsyncNode


class JSONResponse(Node):
    def __init__(self, in_edge, task):
        super().__init__()
        self.in_edge = in_edge
        self.task = task
        self.task.response = {
            "id": self.task.id,
            "name": self.task.id[:8],
            "columns": [],
            "records": [],
            "created": time.strftime("%X %x %Z",
                                     time.localtime(self.task.created)),
            "status": self.task.status
        }

    def run(self):
        self.task.response["records"] = list(self.in_edge.records)
        self.on_finish()

    def in_edges(self):
        return (self.in_edge,)

    def on_finish(self):
        self.status = "done"
        self.task.response["status"] = "done"


class AsyncJSONResponse(AsyncNode):
    def __init__(self, in_edge, task):
        super().__init__()
        self.in_edge = in_edge
        self.task = task
        self.task.response = {
            "id": self.task.id,
            "name": self.task.id[:8],
            "columns": [],
            "records": [],
            "created": time.strftime("%X %x %Z",
                                     time.localtime(self.task.created)),
            "status": self.task.status
        }

    @gen.coroutine
    def _dispatch(self):
        while 1:
            in_ = yield self.in_edge.get()
            self.task.response["records"].append(in_)

    def in_edges(self):
        return (self.in_edge,)

    def on_start(self):
        self.status = "running"
        self.task.response["status"] = "running"

    def on_finish(self):
        self.status == "done"
        self.task.response["status"] = "done"

    def on_aborted(self):
        self.status = "aborted"
        self.task.response["status"] = "aborted"
