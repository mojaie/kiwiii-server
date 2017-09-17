#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen
from kiwiii.node.node import Node, Edge
from kiwiii.worker import Worker


class MPNodeWorker(Worker):
    def __init__(self, args, func, edge):
        super().__init__(args, func)
        self.edge = edge

    def on_task_done(self, record):
        self.edge.data.append(record)


class MPFilter(Node):
    def __init__(self, func, in_edge):
        self.func = func
        self.in_edge = in_edge
        self.out_edge = Edge()

    @gen.coroutine
    def run(self):
        worker = MPNodeWorker(
            self.in_edge, self.func, self.out_edge)
        worker.run()
        yield

    def in_edges(self):
        return (self.in_edge,)

    def out_edges(self):
        return (self.out_edge,)
