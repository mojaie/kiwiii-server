#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools

from kiwiii.core.edge import Edge
from kiwiii.core.task import Task


class Merge(Task):
    def __init__(self, in_edges):
        self._in_edges = in_edges
        self.out_edge = Edge()

    def in_edges(self):
        return self._in_edges

    def out_edges(self):
        return (self.out_edge,)

    def run(self):
        self.out_edge.records = itertools.chain.from_iterable(
            i.records for i in self._in_edges)
        self.on_finish()

    def on_submitted(self):
        self.out_edge.task_count = sum([i.task_count for i in self._in_edges])
