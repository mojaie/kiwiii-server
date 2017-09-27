#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen
from kiwiii.task import Node, AsyncNode, Edge, AsyncQueueEdge


class Synchronizer(AsyncNode):
    def __init__(self, in_edge):
        super().__init__()
        self.in_edge = in_edge
        self.out_edge = Edge()

    @gen.coroutine
    def _dispatch(self):
        while 1:
            in_ = yield self.in_edge.get()
            self.out_edge.records.append(in_)

    def in_edges(self):
        return (self.in_edge,)

    def out_edges(self):
        return (self.out_edge,)

    def on_finish(self):
        self.status = "done"

    def on_aborted(self):
        self.status = "aborted"


class Asynchronizer(Node):
    def __init__(self, in_edge):
        super().__init__()
        self.in_edge = in_edge
        self.out_edge = AsyncQueueEdge()

    @gen.coroutine
    def run(self):
        self.on_start()
        for in_ in self.in_edge.records:
            yield self.out_edge.put(in_)
        yield self.out_edge.done()
        self.on_finish()

    def in_edges(self):
        return (self.in_edge,)

    def out_edges(self):
        return (self.out_edge,)
