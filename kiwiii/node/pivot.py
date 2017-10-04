#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools

from kiwiii.core.node import Node


class Stack(Node):
    def __init__(self, keys, in_edge):
        super().__init__(in_edge)
        self.keys = keys

    def run(self):
        self.out_edge.records = itertools.chain(
            i.records for i in self.in_edges)
        self.on_finish()

    def on_submitted(self):
        self.out_edge.task_count = self.in_edge.task_count * len(self.fields)
