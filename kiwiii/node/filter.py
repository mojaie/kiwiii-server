#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen
from kiwiii.task import Node, Edge, AsyncQueueEdge, MPWorker


class Filter(Node):
    def __init__(self, func, in_edge):
        super().__init__()
        self.func = func
        self.in_edge = in_edge
        self.out_edge = Edge()

    def run(self):
        self.out_edge.records = map(self.func, self.in_edge.records)
        self.on_finish()

    def in_edges(self):
        return (self.in_edge,)

    def out_edges(self):
        return (self.out_edge,)


class MPNodeWorker(MPWorker):
    def __init__(self, args, func, node):
        super().__init__(args, func)
        self.node = node

    @gen.coroutine
    def on_task_done(self, record):
        if record is not None:
            yield self.node.out_edge.put(record)

    @gen.coroutine
    def on_finish(self):
        yield self.node.out_edge.done()
        self.node.on_finish()


class MPFilter(Node):
    def __init__(self, func, in_edge):
        super().__init__()
        self.func = func
        self.in_edge = in_edge
        self.out_edge = AsyncQueueEdge()
        self.worker = None

    @gen.coroutine
    def run(self):
        self.on_start()
        self.worker = MPNodeWorker(self.in_edge.records, self.func, self)
        self.worker.run()

    def in_edges(self):
        return (self.in_edge,)

    def out_edges(self):
        return (self.out_edge,)

    def interrupt(self):
        yield self.worker.interrupt()
