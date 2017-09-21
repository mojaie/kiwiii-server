#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen
from kiwiii.node.node import Node, Edge, AsyncQueueEdge
from kiwiii.worker import Worker


class Filter(Node):
    def __init__(self, func, in_edge):
        self.func = func
        self.in_edge = in_edge
        self.out_edge = Edge()

    def run(self):
        self.out_edge.records = map(self.func, self.in_edge.records)

    def in_edges(self):
        return (self.in_edge,)

    def out_edges(self):
        return (self.out_edge,)


class MPNodeWorker(Worker):
    def __init__(self, args, func, out_edge):
        super().__init__(args, func)
        self.out_edge = out_edge

    @gen.coroutine
    def on_task_done(self, record):
        if record is not None:
            print(record)
            self.out_edge.put(record)


class AsyncFilter(Node):
    def __init__(self, func, in_edge):
        self.func = func
        self.in_edge = in_edge
        self.out_edge = AsyncQueueEdge()

    @gen.coroutine
    def run(self):
        records = []
        if isinstance(self.in_edge, AsyncQueueEdge):
            while self.in_edge.status != "done":
                in_record = yield self.in_edge.get()
                records.append(in_record)
        else:
            records = self.in_edge.records
        worker = MPNodeWorker(records, self.func, self.out_edge)
        worker.run()
        yield

    def in_edges(self):
        return (self.in_edge,)

    def out_edges(self):
        return (self.out_edge,)
