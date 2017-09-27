#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools

from tornado import gen
from kiwiii.task import Node, AsyncNode, Edge, AsyncQueueEdge


class NumberGenerator(Node):
    def __init__(self, in_edge, name="_index"):
        super().__init__()
        self.in_edge = in_edge
        self.out_edge = Edge()
        self.name = name

    def run(self):
        cnt = itertools.count()
        for in_ in self.in_edge.records:
            out = {self.name: next(cnt)}
            out.update(in_)
            self.out_edge.records.append(out)
        self.on_finish()

    def in_edges(self):
        return (self.in_edge,)

    def out_edges(self):
        return (self.out_edge,)


class AsyncNumberGenerator(AsyncNode):
    def __init__(self, in_edge, name="_index"):
        super().__init__()
        self.in_edge = in_edge
        self.out_edge = AsyncQueueEdge()
        self.name = name

    @gen.coroutine
    def _dispatch(self):
        cnt = itertools.count()
        while 1:
            in_ = yield self.in_edge.get()
            out = {self.name: next(cnt)}
            out.update(in_)
            yield self.out_edge.put(out)

    def in_edges(self):
        return (self.in_edge,)

    def out_edges(self):
        return (self.out_edge,)
