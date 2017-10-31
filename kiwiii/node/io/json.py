#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import time

from tornado import gen

from kiwiii import static
from kiwiii.core.node import Node, Synchronizer


class JSONResponse(Node):
    def __init__(self, in_edge, wf):
        super().__init__(in_edge)
        self.wf = wf

    def run(self):
        self.wf.resultRecords = list(self.in_edge.records)
        self.wf.resultCount = len(self.wf.reslutRecords)
        self.wf.doneCount = self.in_edge.task_count
        self.on_finish()

    def out_edges(self):
        return tuple()

    def on_submitted(self):
        # TODO: reorder fields
        self.wf.fields = self.in_edge.fields
        self.wf.doneCount = 0
        self.wf.taskCount = self.in_edge.task_count


class AsyncJSONResponse(Synchronizer):
    def __init__(self, in_edge, wf):
        super().__init__(in_edge)
        self.wf = wf
        self.wf.response = {
            "id": self.wf.id,
            "name": self.wf.id[:8],
            "dataType": self.wf.datatype,
            "schemaVersion": static.SCHEMA_VERSION,
            "revision": 0,
            "query": self.wf.query,
            "fields": [],
            "records": [],
            "created": time.strftime("%X %x %Z",
                                     time.localtime(self.wf.creation_time)),
            "execTime": 0,
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
                self.wf.response["progress"] = round(done / tasks * 100, 1)
            except ZeroDivisionError:
                pass
            exec_time = time.time() - self.wf.start_time
            self.wf.response["execTime"] = round(exec_time, 1)

    def out_edges(self):
        return tuple()

    def on_submitted(self):
        self.wf.response["fields"] = self.wf.fields
        self.wf.response["taskCount"] = self.in_edge.task_count

    def on_start(self):
        self.status = "running"
        self.wf.response["status"] = "running"

    def on_finish(self):
        self.wf.response["status"] = "done"
        # TODO: mpfilter should send doneCount even if the row is filtered out
        self.wf.response["doneCount"] = self.in_edge.task_count
        self.wf.response["progress"] = 100
        exec_time = time.time() - self.wf.start_time
        self.wf.response["execTime"] = round(exec_time, 1)
        self.status = "done"

    def on_aborted(self):
        self.status = "aborted"
        self.wf.response["status"] = "aborted"
        exec_time = time.time() - self.wf.start_time
        self.wf.response["execTime"] = round(exec_time, 1)
