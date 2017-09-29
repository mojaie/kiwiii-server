#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import time

from tornado import gen
from kiwiii.core.node import Node, Synchronizer


class JSONResponse(Node):
    def __init__(self, in_edge, wf):
        super().__init__(in_edge)
        self.wf = wf
        self.wf.response = {
            "id": self.wf.id,
            "name": self.wf.id[:8],
            "fields": [],
            "records": [],
            "created": time.strftime("%X %x %Z",
                                     time.localtime(self.wf.created)),
            "status": self.wf.status,
            "resultCount": 0,
            "taskCount": 0,
            "doneCount": 0,
            "progress": 100
        }

    def run(self):
        self.wf.response["records"] = list(self.in_edge.records)
        self.on_finish()

    def out_edges(self):
        return tuple()

    def on_submitted(self):
        self.wf.response["taskCount"] = self.in_edge.task_count

    def on_finish(self):
        for n in self.wf.nodes:
            n.info(self.wf.response)
        self.wf.response["status"] = "done"
        self.wf.response["resultCount"] = len(self.wf.response["records"])
        self.wf.response["doneCount"] = self.in_edge.task_count
        self.status = "done"


class AsyncJSONResponse(Synchronizer):
    def __init__(self, in_edge, wf):
        super().__init__(in_edge)
        self.wf = wf
        self.wf.response = {
            "id": self.wf.id,
            "name": self.wf.id[:8],
            "fields": [],
            "records": [],
            "created": time.strftime("%X %x %Z",
                                     time.localtime(self.wf.created)),
            "status": self.wf.status,
            "resultCount": 0,
            "taskCount": 0,
            "doneCount": 0,
            "progress": 0
        }

    @gen.coroutine
    def _get_loop(self):
        while 1:
            in_ = yield self.in_edge.get()
            self.wf.response["records"].append(in_)
            self.wf.response["resultCount"] += 1
            tasks = self.wf.response["taskCount"]
            done = self.wf.response["doneCount"] = self.in_edge.done_count
            try:
                self.wf.response["progress"] = round(done / tasks, 1)
            except ZeroDivisionError:
                pass

    def out_edges(self):
        return tuple()

    def on_submitted(self):
        self.wf.response["fields"] = self.wf.fields
        self.wf.response["taskCount"] = self.in_edge.task_count

    def on_start(self):
        self.status = "running"
        self.wf.response["status"] = "running"

    def on_finish(self):
        self.status = "done"
        self.wf.response["status"] = "done"

    def on_aborted(self):
        self.status = "aborted"
        self.wf.response["status"] = "aborted"
