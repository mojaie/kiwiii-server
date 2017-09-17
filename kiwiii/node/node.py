#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen
from tornado.queues import Queue


class Node(object):
    def in_edges(self):
        return tuple()

    def out_edges(self):
        return tuple()


class Edge(object):
    def __init__(self):
        self.source = None
        self.data = []

    def get(self):
        return next(self.data)


class AsyncQueueEdge(Edge):
    def __init__(self):
        self.data = []
        self._queue = Queue(20)
        self.status = "ready"

    @gen.coroutine
    def put(self, record):
        yield self._queue.put()

    @gen.coroutine
    def get(self):
        res = yield self._queue.get()
        return res

    @gen.coroutine
    def done(self):
        yield self._queue.join()
        self.status = "done"
