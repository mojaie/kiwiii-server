#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen
from kiwiii.task import Node, Edge, AsyncQueueEdge


class Synchronizer(Node):
    def __init__(self, in_edge):
        super().__init__()
        self.in_edge = in_edge
        self.out_edge = Edge()

    @gen.coroutine
    def _dispatch(self):
        while 1:
            res = yield self.in_edge.get()
            self.out_edge.records.append(res)

    @gen.coroutine
    def run(self):
        self.on_start()
        self._dispatch()
        while self.in_edge.status != "done":
            yield gen.sleep(0.5)
        self.on_finish()

    def in_edges(self):
        return (self.in_edge,)

    def out_edges(self):
        return (self.out_edge,)


class Asynchronizer(Node):
    def __init__(self, in_edge):
        super().__init__()
        self.in_edge = in_edge
        self.out_edge = AsyncQueueEdge()

    @gen.coroutine
    def run(self):
        self.on_start()
        for rcd in self.in_edge.records:
            yield self.out_edge.put(rcd)
        yield self.out_edge.done()
        self.on_finish()

    def in_edges(self):
        return (self.in_edge,)

    def out_edges(self):
        return (self.out_edge,)
