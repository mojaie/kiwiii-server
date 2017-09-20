#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools

from tornado import gen
from kiwiii.node.node import Node, Edge, AsyncQueueEdge


class NumberGenerator(Node):
    def __init__(self, in_edge, name="_index", params=None):
        self.in_edge = in_edge
        self.out_edge = Edge()
        self.name = name
        self.params = params

    def run(self):
        res = []
        cnt = itertools.count()
        for rcd in self.in_edge.records:
            n = next(cnt)
            newrcd = {self.name: n}
            newrcd.update(rcd)
            res.append(newrcd)
        self.out_edge.records = res

    def in_edges(self):
        return (self.in_edge,)

    def out_edges(self):
        return (self.out_edge,)


class AsyncNumberGenerator(Node):
    def __init__(self, in_edge, name="_index", params=None):
        self.in_edge = in_edge
        self.out_edge = AsyncQueueEdge()
        self.name = name
        self.params = params

    @gen.coroutine
    def run(self):
        cnt = itertools.count()
        while self.in_edge.status != "done":
            in_record = yield self.in_edge.get()
            n = next(cnt)
            yield self.out_edge.put({self.name: n}.update(in_record))

    def in_edges(self):
        return (self.in_edge,)

    def out_edges(self):
        return (self.out_edge,)