#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools

from tornado import gen
from kiwiii.task import Node, Edge, AsyncQueueEdge


class NumberGenerator(Node):
    def __init__(self, in_edge, name="_index"):
        super().__init__()
        self.in_edge = in_edge
        self.out_edge = Edge()
        self.name = name

    def run(self):
        cnt = itertools.count()
        for rcd in self.in_edge.records:
            newrcd = {self.name: next(cnt)}
            newrcd.update(rcd)
            self.out_edge.records.append(newrcd)
        self.on_finish()

    def in_edges(self):
        return (self.in_edge,)

    def out_edges(self):
        return (self.out_edge,)


class AsyncNumberGenerator(Node):
    def __init__(self, in_edge, name="_index"):
        super().__init__()
        self.in_edge = in_edge
        self.out_edge = AsyncQueueEdge()
        self.name = name

    @gen.coroutine
    def run(self):
        self.on_start()
        cnt = itertools.count()
        while self.in_edge.status == "ready":
            if self.status == "interrupted":
                self.on_aborted()
                return
            in_record = yield self.in_edge.get()
            record = {self.name: next(cnt)}
            record.update(in_record)
            yield self.out_edge.put(record)
        yield self.out_edge.done()
        self.on_finish()

    def in_edges(self):
        return (self.in_edge,)

    def out_edges(self):
        return (self.out_edge,)

    def interrupt(self):
        self.status = "interrupted"
        while self.status != "aborted":
            yield gen.sleep(0.5)
