#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen

from kiwiii.core.node import Node, Synchronizer


class JSONResponse(Node):
    def __init__(self, in_edge, wf):
        super().__init__(in_edge)
        self.wf = wf

    def run(self):
        self.wf.result_records = list(self.in_edge.records)
        self.wf.result_count = len(self.wf.result_records)
        self.wf.done_count = self.in_edge.task_count
        self.on_finish()

    def out_edges(self):
        return tuple()

    def on_submitted(self):
        # TODO: reorder fields
        self.wf.fields.merge(self.in_edge.fields)
        self.wf.done_count = 0
        self.wf.task_count = self.in_edge.task_count


class AsyncJSONResponse(Synchronizer):
    def __init__(self, in_edge, wf):
        super().__init__(in_edge)
        self.wf = wf

    @gen.coroutine
    def _get_loop(self):
        while 1:
            in_ = yield self.in_edge.get()
            self.wf.result_records.append(in_)
            self.wf.result_count += 1
            self.wf.done_count = self.in_edge.done_count

    def out_edges(self):
        return tuple()

    def on_submitted(self):
        # TODO: reorder fields
        self.wf.fields.merge(self.in_edge.fields)
        self.wf.result_records = []
        self.wf.result_count = 0
        self.wf.task_count = self.in_edge.task_count
        self.wf.done_count = 0
