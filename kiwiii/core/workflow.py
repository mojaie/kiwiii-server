#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen

from kiwiii.util import graph
from kiwiii.core.node import Asynchronizer
from kiwiii.core.task import Task


class Workflow(Task):
    def __init__(self):
        super().__init__()
        self.nodes = []
        self.preds = {}
        self.succs = {}
        self.order = None
        self.fields = []
        self.interval = 0.5
        self.query = {}
        self.datatype = "nodes"

    @gen.coroutine
    def submit(self):
        self.on_submitted()
        yield self.run()

    @gen.coroutine
    def run(self):
        self.on_start()
        for node_id in self.order:
            self.nodes[node_id].run()
        while any(n.status == "running" for n in self.nodes):
            if self.status == "aborted":
                return
            yield gen.sleep(self.interval)
        self.on_finish()

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
        fs = [
            n.interrupt() for n in self.nodes if isinstance(n, Asynchronizer)]
        while not all(f.done() for f in fs):
            yield gen.sleep(self.interval)
        self.on_aborted()

    def on_submitted(self):
        self.order = graph.topological_sort(self.succs, self.preds)
        for node_id in self.order:
            self.nodes[node_id].on_submitted()
