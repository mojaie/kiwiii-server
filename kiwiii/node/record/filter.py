#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen
from kiwiii.core.task import MPWorker
from kiwiii.core.node import Node, Asynchronizer


class FilterRecords(Node):
    def __init__(self, func, in_edge):
        super().__init__(in_edge)
        self.func = func

    def on_submitted(self):
        self.out_edge.records = filter(self.func, self.in_edge.records)
        self.out_edge.task_count = self.in_edge.task_count


class MPNodeWorker(MPWorker):
    def __init__(self, args, func, node):
        super().__init__(args, func)
        self.node = node

    @gen.coroutine
    def on_task_done(self, record):
        self.node.out_edge.done_count += 1
        if record:
            yield self.node.out_edge.put(record)
        # TODO:
        # if not done_count % 100:
        #     yield self.node.out_edge.proceed()

    @gen.coroutine
    def on_finish(self):
        yield self.node.out_edge.done()
        self.node.on_finish()
        self.status = "done"

    @gen.coroutine
    def on_aborted(self):
        yield self.node.out_edge.aborted()
        self.node.on_aborted()
        self.status = "aborted"


class MPFilterRecords(Asynchronizer):
    def __init__(self, func, in_edge):
        super().__init__(in_edge)
        self.func = func
        self.worker = None

    @gen.coroutine
    def run(self):
        self.on_start()
        self.worker = MPNodeWorker(self.in_edge.records, self.func, self)
        yield self.worker.run()

    @gen.coroutine
    def interrupt(self):
        if self.status != "running":
            return
        yield self.worker.interrupt()
