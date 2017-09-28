#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen

from kiwiii.core.edge import Edge, AsyncQueueEdge
from kiwiii.core.task import Task


class Node(Task):
    """
    Parameters:
      status: str
        ready: ready to run
        done: finished and put all results to outgoing edges
    """
    def __init__(self, in_edge=None):
        super().__init__()
        self.in_edge = in_edge
        self.out_edge = Edge()

    def in_edges(self):
        return (self.in_edge,)

    def out_edges(self):
        return (self.out_edge,)


class FlashNode(Node):
    """For sync node test"""
    def run(self):
        self.on_finish()


class AsyncNode(Task):
    def __init__(self, in_edge=None):
        super().__init__()
        self.in_edge = in_edge
        self.out_edge = AsyncQueueEdge()
        self.interval = 0.5

    @gen.coroutine
    def _get_loop(self):
        while 1:
            in_ = yield self.in_edge.get()
            yield self.out_edge.put(in_)

    @gen.coroutine
    def run(self):
        self.on_start()
        self._get_loop()
        while 1:
            if self.in_edge.status == "aborted":
                self.out_edge.status = "aborted"
                self.on_aborted()
                break
            if self.in_edge.status == "done":
                self.out_edge.status = "done"
                self.on_finish()
                break
            yield gen.sleep(self.interval)

    def in_edges(self):
        return (self.in_edge,)

    def out_edges(self):
        return (self.out_edge,)


class Synchronizer(Task):
    def __init__(self, in_edge=None):
        super().__init__()
        self.in_edge = in_edge
        self.out_edge = Edge()
        self.interval = 0.5

    @gen.coroutine
    def _get_loop(self):
        while 1:
            in_ = yield self.in_edge.get()
            self.out_edge.records.append(in_)

    @gen.coroutine
    def run(self):
        self.on_start()
        self._get_loop()
        while 1:
            if self.in_edge.status == "aborted":
                self.out_edge.status = "aborted"
                self.on_aborted()
                break
            if self.in_edge.status == "done":
                self.out_edge.status = "done"
                self.on_finish()
                break
            yield gen.sleep(self.interval)

    def in_edges(self):
        return (self.in_edge,)

    def out_edges(self):
        return (self.out_edge,)


class LazyConsumer(Synchronizer):
    """For async node test"""
    @gen.coroutine
    def _get_loop(self):
        while 1:
            in_ = yield self.in_edge.get()
            yield gen.sleep(0.01)
            self.out_edge.records.append(in_)


class Asynchronizer(Task):
    def __init__(self, in_edge=None):
        super().__init__()
        self.in_edge = in_edge
        self.out_edge = AsyncQueueEdge()

    @gen.coroutine
    def run(self):
        self.on_start()
        for in_ in self.in_edge.records:
            if self.status == "interrupted":
                return
            yield self.out_edge.put(in_)
        yield self.out_edge.done()
        self.on_finish()

    def in_edges(self):
        return (self.in_edge,)

    def out_edges(self):
        return (self.out_edge,)

    @gen.coroutine
    def interrupt(self):
        if self.status != "running":
            return
        self.status = "interrupted"
        yield self.out_edge.aborted()
        self.on_aborted()


class EagerProducer(Asynchronizer):
    def __init__(self):
        super().__init__()
        self.out_edge = AsyncQueueEdge()

    @gen.coroutine
    def run(self):
        self.on_start()
        for i in range(1000):
            if self.status == "interrupted":
                self.on_aborted()
                return
            yield self.out_edge.put(i)
        yield self.out_edge.done()
        self.on_finish()

    def in_edges(self):
        return tuple()
