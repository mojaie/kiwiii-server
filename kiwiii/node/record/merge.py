#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools

from kiwiii.core.edge import Edge
from kiwiii.core.task import Task


class MergeRecords(Task):
    def __init__(self, in_edges):
        super().__init__()
        self._in_edges = in_edges
        self.out_edge = Edge()

    def in_edges(self):
        return self._in_edges

    def out_edges(self):
        return (self.out_edge,)

    def on_submitted(self):
        self.out_edge.records = itertools.chain.from_iterable(
            i.records for i in self._in_edges)
        self.out_edge.task_count = sum([i.task_count for i in self._in_edges])
        self.out_edge.fields.merge(self._in_edges[0].fields) # TODO
        self.out_edge.params.update(self.in_edge.params)
