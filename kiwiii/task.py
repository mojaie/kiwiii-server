#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from concurrent import futures as cf
import threading
import time
import uuid

from tornado import gen
from tornado.queues import Queue

from kiwiii import static
from kiwiii.util import graph, debug


class Task(object):
    """
    Parameters:
      status: str
        ready, running, done, aborted
        interrupted: method interrupt is called but the task is not yet aborted
        cancelled: cancelled before start
    """
    def __init__(self):
        self.id = str(uuid.uuid4())
        self.status = "ready"
        self.created = time.time()

    def run(self):
        pass

    def on_start(self):
        self.status = "running"

    def on_task_done(self, res):
        pass

    def on_finish(self):
        self.status = "done"

    def on_aborted(self):
        self.status = "aborted"

    def interrupt(self):
        pass

    def size(self):
        return debug.total_size(self)


class MPWorker(Task):
    """General-purpose multiprocess worker
    Args:
        args: iterable task array
        func: task processor
    """
    def __init__(self, args, func):
        super().__init__()
        self.args = args
        self.func = func
        self._queue = Queue(static.PROCESSES * 2)

    @gen.coroutine
    def run(self):
        self.on_start()
        with cf.ThreadPoolExecutor(static.PROCESSES) as tp:
            for p in range(static.PROCESSES):
                tp.submit(self._consumer())
            with cf.ProcessPoolExecutor(static.PROCESSES) as pp:
                for i, a in enumerate(self.args):
                    yield self._queue.put(pp.submit(self.func, a))
                    if self.status == "interrupted":
                        yield self._queue.join()
                        self.on_aborted()
                        return
                yield self._queue.join()
        self.on_finish()

    @gen.coroutine
    def _consumer(self):
        while True:
            f = yield self._queue.get()
            res = yield f
            with threading.Lock():
                self.on_task_done(res)
            self._queue.task_done()

    @gen.coroutine
    def interrupt(self):
        self.status = "interrupted"
        while self.status != "aborted":
            yield gen.sleep(0.5)


class Node(Task):
    """
    Parameters:
      status: str
        ready: ready to run
        done: finished and put all results to outgoing edges
    """
    def __init__(self):
        super().__init__()

    def in_edges(self):
        return tuple()

    def out_edges(self):
        return tuple()


class AsyncNode(Node):
    """
    Parameters:
      status: str
        ready: ready to run
        done: finished and put all results to outgoing edges
    """
    def __init__(self):
        super().__init__()
        self.interval = 0.5

    @gen.coroutine
    def _dispatch(self):
        while 1:
            in_ = yield self.in_edge.get()
            yield self.out_edge.put(in_)

    @gen.coroutine
    def run(self):
        self.on_start()
        self._dispatch()
        while 1:
            if self.status == "interrupted":
                self.on_aborted()
                break
            if self.in_edge.status == "done":
                self.on_finish()
                break
            yield gen.sleep(self.interval)

    @gen.coroutine
    def interrupt(self):
        self.status = "interrupted"
        while self.status != "aborted":
            yield gen.sleep(self.interval)

    @gen.coroutine
    def on_finish(self):
        yield self.out_edge.done()
        self.status = "done"

    @gen.coroutine
    def on_aborted(self):
        yield self.out_edge.done()
        self.status = "aborted"


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


class Workflow(Task):
    def __init__(self):
        super().__init__()
        self.nodes = []
        self.preds = {}
        self.succs = {}
        self.interval = 0.5

    @gen.coroutine
    def run(self):
        self.on_start()
        order = graph.topological_sort(self.succs, self.preds)
        for node_id in order:
            # yield gen.maybe_future(self.nodes[node_id].run())
            self.nodes[node_id].run()
        while self.status == "running":
            if all(n.status == "done" for n in self.nodes):
                self.on_finish()
                break
            if self.status == "interrupted":
                break
            yield gen.sleep(self.interval)

    def add_node(self, node):
        """parallel computation
        returns worker id
        """
        node_id = len(self.nodes)
        self.nodes.append(node)
        self.preds[node_id] = {}
        self.succs[node_id] = {}
        for in_ in node.in_edges():
            self.preds[node_id][in_.source] = True
            self.succs[in_.source][node_id] = True
        for out in node.out_edges():
            out.source = node_id
        return node.out_edges()

    @gen.coroutine
    def interrupt(self):
        self.status = "interrupted"
        for node in self.nodes:
            yield gen.maybe_future(node.interrupt())
        self.on_aborted()
