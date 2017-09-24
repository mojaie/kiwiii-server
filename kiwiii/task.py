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
    def __init__(self):
        self.id = str(uuid.uuid4())
        self.status = "ready"
        self.created = time.time()
        self.interruption_requested = False

    def run(self):
        pass

    def on_start(self):
        pass

    def on_task_done(self, res):
        pass

    def on_finish(self):
        pass

    def on_interrupted(self):
        pass

    def interrupt(self):
        print("Interruption requested...")
        self.interruption_requested = True

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
                    if self.interruption_requested:
                        yield self._queue.join()
                        self.on_interrupted()
                        return
                yield self._queue.join()
        self.status = -1
        self.on_finish()

    @gen.coroutine
    def _consumer(self):
        while True:
            f = yield self._queue.get()
            res = yield f
            with threading.Lock():
                self.on_task_done(res)
            self._queue.task_done()


class Node(object):
    def __init__(self):
        self.status = "ready"

    def in_edges(self):
        return tuple()

    def out_edges(self):
        return tuple()

    def interrupt(self):
        pass


class Edge(object):
    def __init__(self):
        self.records = []
        self.status = "ready"


class AsyncQueueEdge(Edge):
    def __init__(self):
        self.records = []
        self.status = "ready"
        self._queue = Queue(20)

    @gen.coroutine
    def put(self, record):
        yield self._queue.put(record)

    @gen.coroutine
    def get(self):
        res = yield self._queue.get()
        return res

    @gen.coroutine
    def done(self):
        yield self._queue.join()
        self.status = "done"


class Workflow(Task):
    def __init__(self):
        super().__init__()
        self.nodes = []
        self.preds = {}
        self.succs = {}

    @gen.coroutine
    def run(self):
        order = graph.topological_sort(self.succs, self.preds)
        for node_id in order:
            # yield gen.maybe_future(self.nodes[node_id].run())
            self.nodes[node_id].run()
        while 1:
            if self.interruption_requested:
                print("Workflow interrupted")
                self.on_interrupted()
                break
            print("id {}".format(self.id))
            print([n.status == "done" for n in self.nodes])
            if all(n.status == "done" for n in self.nodes):
                self.on_finish()
                break
            print("Waiting...")
            yield gen.sleep(0.5)

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

    def on_interrupted(self):
        for node in self.nodes:
            node.interrupt()
