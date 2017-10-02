#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from kiwiii.core.node import Node
from kiwiii.core.edge import Edge


class IteratorInput(Node):
    def __init__(self, rcds):
        super().__init__()
        self.out_edge = Edge()
        self.records = rcds
        self.out_edge.task_count = len(list(rcds))

    def run(self):
        self.out_edge.records = self.records
        self.on_finish()

    def in_edges(self):
        return tuple()
