#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen
from tornado.queues import Queue


class Edge(object):
    """
    Parameters:
      status: str
        ready: ready to put results
        done: incoming node finished its task and put all results to the edge
    """
    def __init__(self):
        self.records = []


class AsyncQueueEdge(object):
    def __init__(self):
        self.queue = Queue(20)
        self.status = "ready"

    @gen.coroutine
    def put(self, record):
        yield self.queue.put(record)

    @gen.coroutine
    def get(self):
        res = yield self.queue.get()
        self.queue.task_done()
        return res

    @gen.coroutine
    def done(self):
        yield self.queue.join()
        self.status = "done"

    @gen.coroutine
    def aborted(self):
        yield self.queue.join()
        self.status = "aborted"
