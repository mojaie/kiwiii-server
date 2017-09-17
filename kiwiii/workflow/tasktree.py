#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import time

from tornado import gen
from kiwiii.worker import Worker
from kiwiii.util import graph


class TaskTree(Worker):
    def __init__(self):
        self.nodes = []
        self.preds = {}
        self.succs = {}
        self.start_date = None

    @gen.coroutine
    def run(self):
        self.start_date = time.time()
        order = graph.topological_sort(self.succs, self.preds)
        for node_id in order:
            print("Started task {}".format(node_id))
            yield self.nodes[node_id].run()
            print("Finished task {}".format(node_id))

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
